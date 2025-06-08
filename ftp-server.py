import socket
import os
import threading
import logging
import json
import shutil # Added for copy and rmtree

# Base directory where all user data will be stored
# This is separate from the server code's directory
base_user_data_dir = os.path.join(os.getcwd(), 'users_data') # Renamed for clarity

server_running = True  # Variable to control server state
client_threads = []  # List to store client threads
server_lock = threading.Lock()  # Lock for server state synchronization

# Logging setup
# Configure loggers to prevent propagation to root and duplicate messages
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

file_logger = logging.getLogger('file_logger')
file_handler = logging.FileHandler('file_operations.log')
file_logger.addHandler(file_handler)
file_logger.propagate = False # Prevent logs from going to root logger again

auth_logger = logging.getLogger('auth_logger')
auth_handler = logging.FileHandler('auth.log')
auth_logger.addHandler(auth_handler)
auth_logger.propagate = False

conn_logger = logging.getLogger('conn_logger')
conn_handler = logging.FileHandler('connections.log')
conn_logger.addHandler(conn_handler)
conn_logger.propagate = False

# Load user information from file
def load_users():
    if os.path.exists('users.json'):
        with open('users.json', 'r') as f:
            return json.load(f)
    else:
        return {}

# Save user information to file
def save_users(users):
    with open('users.json', 'w') as f:
        json.dump(users, f)

# Authenticate user
def authenticate_user(username, password):
    users = load_users()
    if username in users and users[username]['password'] == password:
        auth_logger.info(f"User {username} authenticated successfully")
        return True
    else:
        auth_logger.warning(f"Authentication failed for user {username}")
        return False

# Register new user
def register_user(username, password):
    users = load_users()
    if username in users:
        return False
    else:
        # Create base directory for all user data if it doesn't exist
        if not os.path.exists(base_user_data_dir):
            os.makedirs(base_user_data_dir)
            file_logger.info(f"Created base user data directory: {base_user_data_dir}")

        # Create user's specific base directory (e.g., users_data/username/)
        user_base_dir = os.path.join(base_user_data_dir, username)
        os.makedirs(user_base_dir)
        
        # Create the 'docs' directory inside the user's base directory
        # This will be the actual root for user's file operations
        user_docs_dir = os.path.join(user_base_dir, 'docs')
        os.makedirs(user_docs_dir)
        
        users[username] = {'password': password, 'quota': 1024 * 1024 * 10}  # 10 MB quota
        save_users(users)
        auth_logger.info(f"New user {username} registered. User docs directory created at {user_docs_dir}")
        return True

def get_safe_path(base_dir, relative_path):
    """
    Constructs a safe absolute path within a confined base directory.
    Prevents path traversal attacks (e.g., using '..').
    Returns None if the relative_path attempts to escape the base_dir.
    """
    # Normalize base_dir first to ensure it's an absolute, canonical path
    abs_base_dir = os.path.realpath(base_dir)

    # Join the base directory with the relative path from the client
    # os.path.join handles different path separators and resolves 'foo/./bar'
    abs_path = os.path.realpath(os.path.join(abs_base_dir, relative_path))

    # Crucial check: Ensure the constructed path is actually within the base directory
    # If not, it means 'relative_path' tried to escape abs_base_dir (e.g., using '..')
    if not abs_path.startswith(abs_base_dir):
        file_logger.warning(f"Path traversal attempt: {relative_path} from {abs_base_dir}")
        return None 
    
    return abs_path

# Function to process client requests (excluding file transfers, exit, stop)
# ... (previous code) ...

# Function to process client requests (excluding file transfers, exit, stop)
def process_command(req, username):
    req_parts = req.split()
    command = req_parts[0].lower()
    
    # Define the user's actual working directory for file operations
    user_docs_dir = os.path.join(base_user_data_dir, username, 'docs')

    # Basic argument validation
    if command in ['mkdir', 'rmdir', 'rmfile', 'rename', 'copy'] and len(req_parts) < 2:
        return f"Usage: {command} <argument(s)>"
    if command in ['rename', 'copy'] and len(req_parts) < 3:
        return f"Usage: {command} <source> <destination>"

    if command == 'pwd':
        # Return a confined view of the directory
        return "Current directory: /" # Represents the root of the user's docs folder
    elif command == 'ls':
        target_dir = user_docs_dir
        if len(req_parts) > 1:
            safe_target_dir = get_safe_path(user_docs_dir, req_parts[1])
            if safe_target_dir is None or not os.path.isdir(safe_target_dir):
                return f"Error: Directory '{req_parts[1]}' does not exist or is not accessible."
            target_dir = safe_target_dir
            
        try:
            items = os.listdir(target_dir)
            if not items: # If directory is empty, return a specific message
                return "(empty directory)"
            else:
                return '; '.join(items)
        except OSError as e:
            return f"Error listing directory: {e}"

    # ... (rest of the process_command function) ...

    elif command == 'mkdir':
        dirname_client = req_parts[1]
        safe_dirname = get_safe_path(user_docs_dir, dirname_client)
        if safe_dirname is None:
            return f"Access denied: Cannot create directory '{dirname_client}' outside your designated area."
        
        try:
            if not os.path.exists(safe_dirname):
                os.makedirs(safe_dirname)
                file_logger.info(f"User {username} created directory: {safe_dirname}")
                return f"Directory created: {dirname_client}"
            else:
                return "Directory already exists"
        except OSError as e:
            return f"Error creating directory {dirname_client}: {e}"

    elif command == 'rmdir':
        dirname_client = req_parts[1]
        safe_dirname = get_safe_path(user_docs_dir, dirname_client)
        if safe_dirname is None:
            return f"Access denied: Cannot remove directory '{dirname_client}' outside your designated area."
        
        try:
            if os.path.exists(safe_dirname) and os.path.isdir(safe_dirname):
                shutil.rmtree(safe_dirname) # Recursively remove directory
                file_logger.info(f"User {username} removed directory (recursively): {safe_dirname}")
                return f"Directory removed: {dirname_client}"
            else:
                return "Directory does not exist or is not a directory."
        except OSError as e:
            return f"Error removing directory {dirname_client}: {e}. It might be in use or you lack permissions."

    elif command == 'rmfile':
        filename_client = req_parts[1]
        safe_filename = get_safe_path(user_docs_dir, filename_client)
        if safe_filename is None:
            return f"Access denied: Cannot remove file '{filename_client}' outside your designated area."

        try:
            if os.path.exists(safe_filename) and os.path.isfile(safe_filename):
                os.remove(safe_filename)
                file_logger.info(f"User {username} removed file: {safe_filename}")
                return f"File removed: {filename_client}"
            else:
                return "File does not exist or is not a file."
        except OSError as e:
            return f"Error removing file {filename_client}: {e}. It might be in use or you lack permissions."

    elif command == 'rename':
        old_name_client = req_parts[1] 
        new_name_client = req_parts[2]
        
        safe_old_path = get_safe_path(user_docs_dir, old_name_client)
        safe_new_path = get_safe_path(user_docs_dir, new_name_client)
        
        if safe_old_path is None or safe_new_path is None:
            return "Access denied: Cannot rename paths outside your designated area."
        
        try:
            if os.path.exists(safe_old_path):
                os.rename(safe_old_path, safe_new_path)
                file_logger.info(f"User {username} renamed {safe_old_path} to {safe_new_path}")
                return f"Renamed from {old_name_client} to {new_name_client}"
            else:
                return "Source file/directory does not exist."
        except OSError as e:
            return f"Error renaming {old_name_client} to {new_name_client}: {e}"
            
    elif command == 'copy':
        source_client = req_parts[1]
        destination_client = req_parts[2]

        safe_source_path = get_safe_path(user_docs_dir, source_client)
        safe_destination_path = get_safe_path(user_docs_dir, destination_client)

        if safe_source_path is None or safe_destination_path is None:
            return "Access denied: Cannot copy paths outside your designated area."

        try:
            if not os.path.exists(safe_source_path):
                return "Source file/directory does not exist."
            
            # If destination is a directory, copy source inside it
            if os.path.isdir(safe_destination_path):
                safe_destination_path = os.path.join(safe_destination_path, os.path.basename(safe_source_path))

            if os.path.isfile(safe_source_path):
                shutil.copy2(safe_source_path, safe_destination_path) # copy2 preserves metadata
                file_logger.info(f"User {username} copied file from {safe_source_path} to {safe_destination_path}")
                return f"Copied file from '{source_client}' to '{destination_client}'"
            elif os.path.isdir(safe_source_path):
                # For directories, shutil.copytree is needed, but handle it carefully for overwrite
                # If destination exists and is a file, copytree will fail
                if os.path.exists(safe_destination_path) and os.path.isfile(safe_destination_path):
                    return f"Error: Cannot copy directory '{source_client}' to existing file '{destination_client}'."
                
                # If destination already exists as a directory, copytree requires it not to exist
                if os.path.exists(safe_destination_path):
                    # We can't directly copytree into an existing dir unless merging,
                    # which is more complex. For simplicity, we'll error if dest exists.
                    return f"Error: Destination directory '{destination_client}' already exists. Please provide a non-existent path for directory copy."
                
                shutil.copytree(safe_source_path, safe_destination_path)
                file_logger.info(f"User {username} copied directory from {safe_source_path} to {safe_destination_path}")
                return f"Copied directory from '{source_client}' to '{destination_client}'"
            else:
                return "Source is neither a file nor a directory."
        except shutil.Error as e:
            return f"Error copying {source_client} to {destination_client}: {e}"
        except OSError as e:
            return f"Error copying {source_client} to {destination_client}: {e}"

    else:
        return 'bad request'

def handle_client(conn, addr):
    conn_logger.info(f"Connected by {addr}")
    username = None

    while True:
        try:
            # Phase 1: Authentication or Registration
            if not username:
                request = conn.recv(1024).decode()
                if not request: # Client disconnected
                    conn_logger.info(f"Client {addr} disconnected during authentication phase.")
                    break
                conn_logger.info(f"Received initial request from {addr}: {request}")

                req_parts = request.split()
                if len(req_parts) == 3:
                    action, received_username, password = req_parts
                    if action == 'login':
                        if authenticate_user(received_username, password):
                            conn.send("Authenticated".encode())
                            username = received_username 
                            conn_logger.info(f"User {username} authenticated from {addr}")
                        else:
                            conn.send("Authentication failed".encode())
                    elif action == 'register':
                        if register_user(received_username, password):
                            conn.send("Registered".encode())
                            username = received_username 
                            conn_logger.info(f"New user {username} registered from {addr}")
                        else:
                            conn.send("Registration failed. User may already exist.".encode())
                else:
                    conn.send("Bad request: Format 'login <username> <password>' or 'register <username> <password>'".encode())
            # Phase 2: Handle authenticated commands
            else:
                request = conn.recv(1024).decode()
                if not request: # Client disconnected
                    conn_logger.info(f"Client {username} from {addr} disconnected.")
                    break
                conn_logger.info(f"Received command from {username}@{addr}: {request}")

                command_parts = request.split()
                if not command_parts: # Empty request
                    conn.send("".encode())
                    continue

                command = command_parts[0].lower()
                user_docs_dir = os.path.join(base_user_data_dir, username, 'docs')

                # Ensure user's docs directory exists (safety check, should be created on registration)
                if not os.path.exists(user_docs_dir):
                    conn.send(f"Error: Your user data directory '{user_docs_dir}' does not exist. Please contact support.".encode())
                    file_logger.error(f"User docs directory missing for {username} at {user_docs_dir}")
                    break

                if command == 'upload':
                    if len(command_parts) < 2:
                        conn.send("Usage: upload <filename>".encode())
                        continue
                    filename_client = command_parts[1]
                    safe_filepath = get_safe_path(user_docs_dir, filename_client)
                    
                    if safe_filepath is None:
                        conn.send(f"Access denied: Cannot upload to '{filename_client}' outside your designated area.".encode())
                        continue

                    # Handshake for upload: Server tells client it's ready for size
                    conn.send("READY_FOR_UPLOAD_SIZE".encode()) 

                    # Server waits for file size
                    file_size_str = conn.recv(1024).decode()
                    try:
                        file_size = int(file_size_str)
                    except ValueError:
                        conn.send("Invalid file size provided by client. Aborting upload.".encode())
                        file_logger.warning(f"User {username} sent invalid file size: {file_size_str}")
                        continue 

                    users = load_users() # Reload users for up-to-date quota
                    user_quota = users[username]['quota']
                    
                    if file_size > user_quota:
                        conn.send("Insufficient quota".encode())
                        file_logger.warning(f"User {username} tried to upload {file_size} bytes, but only has {user_quota} bytes quota.")
                    else:
                        conn.send("QUOTA_OK".encode()) # Signal client to send file data
                        users[username]['quota'] -= file_size # Deduct quota
                        save_users(users) 

                        with open(safe_filepath, 'wb') as f:
                            received_bytes = 0
                            while received_bytes < file_size:
                                data = conn.recv(1024)
                                if not data: # Client disconnected during upload
                                    file_logger.error(f"User {username} disconnected during upload of {filename_client}. Incomplete file.")
                                    # TODO: Consider adding logic to revert quota or mark file as incomplete/corrupt
                                    break
                                f.write(data)
                                received_bytes += len(data)
                            
                            if received_bytes == file_size:
                                file_logger.info(f"User {username} uploaded file: {safe_filepath} ({received_bytes} bytes)")
                                conn.send(f"File '{filename_client}' uploaded successfully.".encode())
                            else:
                                file_logger.error(f"User {username} upload of {filename_client} failed. Expected {file_size}, received {received_bytes}. Reverting quota.")
                                # Attempt to revert quota if upload was incomplete
                                users = load_users()
                                users[username]['quota'] += (file_size - received_bytes) # Revert only the difference
                                save_users(users)
                                conn.send(f"Error: Incomplete upload for '{filename_client}'. Please try again.".encode())
                                # Clean up partially uploaded file
                                if os.path.exists(safe_filepath):
                                    os.remove(safe_filepath)
                                    file_logger.info(f"Cleaned up incomplete file: {safe_filepath}")


                elif command == 'download':
                    if len(command_parts) < 2:
                        conn.send("Usage: download <filename>".encode())
                        continue
                    filename_client = command_parts[1]
                    safe_filepath = get_safe_path(user_docs_dir, filename_client)
                    
                    if safe_filepath is None:
                        conn.send(f"Access denied: Cannot download '{filename_client}' from outside your designated area.".encode())
                        continue

                    if os.path.exists(safe_filepath) and os.path.isfile(safe_filepath):
                        file_size = os.path.getsize(safe_filepath)
                        # Handshake for download: Server sends file size first
                        conn.send(f"DOWNLOAD_READY {file_size}".encode()) 
                        
                        # Client is expected to receive this and then read file data
                        with open(safe_filepath, 'rb') as f:
                            while True:
                                data = f.read(1024)
                                if not data:
                                    break
                                conn.sendall(data)
                        file_logger.info(f"User {username} downloaded file: {safe_filepath}")
                    else:
                        conn.send("File does not exist or is a directory.".encode())

                elif command == 'exit':
                    conn.send("exit".encode())
                    break # Exit handle_client loop

                elif command == 'stop':
                    if username == 'admin':
                        with server_lock:
                            global server_running
                            server_running = False
                        conn.send("Server stopping".encode())
                        break # Exit handle_client loop
                    else:
                        conn.send("Insufficient privileges.".encode())

                else: # Other commands (pwd, ls, mkdir, rmdir, rmfile, rename, copy)
                    response = process_command(request, username)
                    conn.send(response.encode())
        
        except socket.error as e:
            conn_logger.error(f"Socket error for {username if username else addr}: {e}")
            break # Break on socket errors (e.g., client disconnected unexpectedly)
        except Exception as e:
            conn_logger.error(f"Unhandled error in handle_client for {username if username else addr} with request '{request if 'request' in locals() else 'N/A'}': {e}", exc_info=True)
            try:
                conn.send(f"Server error: {e}".encode()) # Send error back to client
            except socket.error:
                pass # Client might have already disconnected
            break 

    conn.close()
    conn_logger.info(f"Disconnected from {username if username else addr}")

# Main function to run the server
def main():
    global server_running
    PORT = 6666

    # Create base_user_data_dir if it doesn't exist
    if not os.path.exists(base_user_data_dir):
        os.makedirs(base_user_data_dir)
        conn_logger.info(f"Created base user data directory: {base_user_data_dir}")
        
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) # Specify AF_INET and SOCK_STREAM
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        sock.bind(('', PORT))
        sock.listen()
        sock.settimeout(1.0)  # Set timeout for accept function
        conn_logger.info(f"Listening on port {PORT}")
    except socket.error as e:
        conn_logger.critical(f"Failed to bind or listen on port {PORT}: {e}")
        return # Exit if server cannot start

    while True:
        with server_lock:
            if not server_running:
                break
        try:
            conn, addr = sock.accept()
            client_thread = threading.Thread(target=handle_client, args=(conn, addr))
            client_thread.start()
            client_threads.append(client_thread)
        except socket.timeout:
            # No connection within timeout, loop continues to check server_running status
            continue
        except Exception as e: 
            conn_logger.error(f"Error accepting connections: {e}", exc_info=True)
            # For unhandled errors during accept, typically fatal, so break.
            break 

    # Wait for all client threads to finish before closing socket
    conn_logger.info("Server shutting down. Waiting for client threads to finish...")
    for thread in client_threads:
        thread.join()
    conn_logger.info("All client threads finished.")

    sock.close()
    conn_logger.info("Server socket closed. Server stopped.")

if __name__ == "__main__":
    main()