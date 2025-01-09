import streamlit as st
import sqlite3
import hashlib
from datetime import datetime
import pika
import json
import threading
import queue
import time

# Import your existing stock analysis function
from tools.fetch_stock_info import Analyze_stock

# RabbitMQ Configuration
RABBITMQ_HOST = 'localhost'
RABBITMQ_QUEUE = 'stock_analysis_queue'
RABBITMQ_RESULT_QUEUE = 'stock_analysis_result_queue'

# Initialize session state variables
if 'result_queue' not in st.session_state:
    st.session_state.result_queue = queue.Queue()

if 'pending_requests' not in st.session_state:
    st.session_state.pending_requests = {}

if 'worker_started' not in st.session_state:
    st.session_state.worker_started = False

if 'listener_started' not in st.session_state:
    st.session_state.listener_started = False

# RabbitMQ Connection and Channel Setup
def get_rabbitmq_connection():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
        return connection
    except Exception as e:
        st.error(f"RabbitMQ Connection Error: {e}")
        return None

# Function to send analysis request to RabbitMQ
def send_analysis_request(query, username):
    try:
        connection = get_rabbitmq_connection()
        if not connection:
            return None
        
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        channel.queue_declare(queue=RABBITMQ_RESULT_QUEUE, durable=True)
        
        # Generate a unique request ID
        request_id = f"{username}_{int(time.time())}"
        
        # Prepare message
        message = {
            'request_id': request_id,
            'query': query,
            'username': username
        }
        
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # make message persistent
                reply_to=RABBITMQ_RESULT_QUEUE
            )
        )
        
        connection.close()
        return request_id
    except Exception as e:
        st.error(f"Error sending message to RabbitMQ: {e}")
        return None

# Database initialization
def init_db():
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (username TEXT PRIMARY KEY, 
                  password TEXT,
                  email TEXT,
                  full_name TEXT,
                  created_at DATETIME)''')
    c.execute('''CREATE TABLE IF NOT EXISTS queries
                 (username TEXT, query TEXT, response TEXT, timestamp DATETIME)''')
    conn.commit()
    conn.close()

def make_hash(password):
    return hashlib.sha256(str.encode(password)).hexdigest()

def check_credentials(username, password):
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute('SELECT password FROM users WHERE username=?', (username,))
    stored_password = c.fetchone()
    conn.close()
    if stored_password:
        return stored_password[0] == make_hash(password)
    return False

def username_exists(username):
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute('SELECT username FROM users WHERE username=?', (username,))
    result = c.fetchone()
    conn.close()
    return result is not None

def email_exists(email):
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute('SELECT email FROM users WHERE email=?', (email,))
    result = c.fetchone()
    conn.close()
    return result is not None

def save_query(username, query, response):
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute('INSERT INTO queries VALUES (?, ?, ?, ?)', 
              (username, query, response, datetime.now()))
    conn.commit()
    conn.close()

def get_user_history(username):
    conn = sqlite3.connect('users.db')
    c = conn.cursor()
    c.execute('SELECT query, response, timestamp FROM queries WHERE username=? ORDER BY timestamp DESC', 
              (username,))
    history = c.fetchall()
    conn.close()
    return history

# Background worker to process RabbitMQ messages
def rabbitmq_worker(result_queue):
    try:
        connection = get_rabbitmq_connection()
        if not connection:
            print("Failed to establish RabbitMQ connection")
            return
        
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        channel.queue_declare(queue=RABBITMQ_RESULT_QUEUE, durable=True)
        
        print(f"[*] Listening on queue: {RABBITMQ_QUEUE}")
        
        def callback(ch, method, properties, body):
            try:
                print(f"[x] Received message: {body}")
                
                # Process the message
                message = json.loads(body)
                query = message['query']
                username = message['username']
                request_id = message['request_id']
                
                print(f"Processing query: {query} for user: {username}")
                
                # Perform stock analysis
                analysis_result = Analyze_stock(query)
                
                # Save query to history
                save_query(username, query, analysis_result)
                
                # Publish result to result queue
                result_message = {
                    'request_id': request_id,
                    'query': query,
                    'username': username,
                    'result': analysis_result
                }
                
                channel.basic_publish(
                    exchange='',
                    routing_key=RABBITMQ_RESULT_QUEUE,
                    body=json.dumps(result_message),
                    properties=pika.BasicProperties(
                        delivery_mode=2
                    )
                )
                
                print("[x] Message processed and result published")
                
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                print(f"Error processing message: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        # Set up consumer with prefetch
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=RABBITMQ_QUEUE, 
            on_message_callback=callback
        )
        
        print(" [*] Waiting for messages. To exit press CTRL+C")
        channel.start_consuming()
    
    except Exception as e:
        print(f"RabbitMQ Worker Error: {e}")

# Result listener function
def result_listener(result_queue):
    try:
        connection = get_rabbitmq_connection()
        if not connection:
            print("Failed to establish RabbitMQ connection for result listener")
            return
        
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_RESULT_QUEUE, durable=True)
        
        def callback(ch, method, properties, body):
            try:
                result_message = json.loads(body)
                result_queue.put(result_message)
                ch.basic_ack(delivery_tag=method.delivery_tag)
            except Exception as e:
                print(f"Error in result listener: {e}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        channel.basic_consume(
            queue=RABBITMQ_RESULT_QUEUE, 
            on_message_callback=callback
        )
        
        channel.start_consuming()
    except Exception as e:
        print(f"Result Listener Error: {e}")

def main():
    # Start RabbitMQ worker thread if not started
    if not st.session_state.worker_started:
        worker_thread = threading.Thread(
            target=rabbitmq_worker, 
            args=(st.session_state.result_queue,), 
            daemon=True
        )
        worker_thread.start()
        st.session_state.worker_started = True
        print("RabbitMQ worker thread started")
    
    # Start result listener thread if not started
    if not st.session_state.listener_started:
        listener_thread = threading.Thread(
            target=result_listener, 
            args=(st.session_state.result_queue,), 
            daemon=True
        )
        listener_thread.start()
        st.session_state.listener_started = True
        print("Result listener thread started")

    # Initialize session state for pending_requests if not already done
    if 'pending_requests' not in st.session_state:
        st.session_state.pending_requests = {}

    # Initialize session state for results
    if 'received_results' not in st.session_state:
        st.session_state.received_results = []

    # Initialize session state for user
    if 'logged_in' not in st.session_state:
        st.session_state.logged_in = False

    # Initialize database
    init_db()

    # Sidebar for authentication
    with st.sidebar:
        if not st.session_state.logged_in:
            st.header("Login/Register")
            action = st.radio("Choose action:", ["Login", "Register"])
        
            if action == "Register":
                st.subheader("Create New Account")
                with st.form("registration_form"):
                    new_username = st.text_input("Username*")
                    new_email = st.text_input("Email*")
                    new_full_name = st.text_input("Full Name*")
                    new_password = st.text_input("Password*", type="password")
                    confirm_password = st.text_input("Confirm Password*", type="password")
                
                    submit_button = st.form_submit_button("Register")
                
                    if submit_button:
                        if not all([new_username, new_email, new_full_name, new_password, confirm_password]):
                            st.error("All fields are required!")
                        elif len(new_password) < 6:
                            st.error("Password must be at least 6 characters long!")
                        elif new_password != confirm_password:
                            st.error("Passwords do not match!")
                        elif username_exists(new_username):
                            st.error("Username already exists!")
                        elif email_exists(new_email):
                            st.error("Email already registered!")
                        elif '@' not in new_email:
                            st.error("Please enter a valid email address!")
                        else:
                            conn = sqlite3.connect('users.db')
                            c = conn.cursor()
                            c.execute('INSERT INTO users VALUES (?, ?, ?, ?, ?)', 
                                    (new_username, make_hash(new_password), 
                                    new_email, new_full_name, datetime.now()))
                            conn.commit()
                            conn.close()
                            st.success("Registration successful! Please login.")
        
            else:  # Login
                with st.form("login_form"):
                    username = st.text_input("Username")
                    password = st.text_input("Password", type="password")
                    login_button = st.form_submit_button("Login")
                
                    if login_button:
                        if check_credentials(username, password):
                            st.session_state.logged_in = True
                            st.session_state.username = username
                            st.success(f"Logged in as {username}")
                            st.experimental_rerun()
                        else:
                            st.error("Invalid credentials")
        else:
            st.write(f"Logged in as {st.session_state.username}")
            if st.button("Logout"):
                st.session_state.logged_in = False
                st.experimental_rerun()

    # Main app logic
    if st.session_state.logged_in:
        st.title("Stock Analysis Bot with RabbitMQ")
        st.write("Asynchronous stock analysis using message queuing")

        # Query input
        query = st.text_input('Input your investment related query:')
        col1, col2 = st.columns(2)
        
        with col1:
            enter = st.button("Enter")
        with col2:
            clear = st.button("Clear")

        if clear:
            st.experimental_rerun()

        if enter and query:
            # Send analysis request to RabbitMQ
            with st.spinner('Queuing analysis request...'):
                request_id = send_analysis_request(
                    query, 
                    st.session_state.username
                )
                if request_id:
                    st.success('Analysis request queued successfully!')
                    # Track the pending request
                    st.session_state.pending_requests[request_id] = {
                        'query': query,
                        'status': 'Pending'
                    }
                else:
                    st.error('Failed to queue analysis request')

        # Poll the result_queue for new results
        while not st.session_state.result_queue.empty():
            try:
                result = st.session_state.result_queue.get_nowait()
                request_id = result['request_id']
                
                # Check if the result is for the current user
                if result['username'] == st.session_state.username:
                    # Update the pending_requests status
                    if request_id in st.session_state.pending_requests:
                        st.session_state.pending_requests[request_id]['status'] = 'Completed'
                    
                    # Optionally, store received results
                    st.session_state.received_results.append(result)
                    
                    # Display the result
                    st.success("Analysis Result Received!")
                    st.write(f"**Query:** {result['query']}")
                    st.write(f"**Result:** {result['result']}")
                    
            except queue.Empty:
                break

        # Display pending requests
        if st.session_state.pending_requests:
            st.subheader("Pending Requests")
            to_remove = []
            for req_id, details in st.session_state.pending_requests.items():
                st.write(f"**Query:** {details['query']} - **Status:** {details['status']}")
                if details['status'] == 'Completed':
                    to_remove.append(req_id)
            # Remove completed requests if desired
            for req_id in to_remove:
                del st.session_state.pending_requests[req_id]

        # History section
        st.header("Your Query History")
        history = get_user_history(st.session_state.username)
        
        for query, response, timestamp in history:
            with st.expander(f"Query: {query[:50]}... ({timestamp})"):
                st.write("**Query:**", query)
                st.write("**Response:**", response)
                st.write("**Time:**", timestamp)
    else:
        st.title("Welcome to Stock Analysis Bot")
        st.write("Please login or register to continue")

if __name__ == "__main__":
    main()
