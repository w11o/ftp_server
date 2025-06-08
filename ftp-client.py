import socket
import os

HOST = 'localhost'
PORT = 6666

def send_file(sock, filepath):
    """Handles sending a file to the server with proper handshakes."""
    try:
        file_size = os.path.getsize(filepath)
        
        # 1. Client waits for server to be ready for size
        response = sock.recv(1024).decode()
        if response != "READY_FOR_UPLOAD_SIZE":
            print(f"Server not ready for upload size: {response}")
            return False

        # 2. Client sends file size
        sock.send(str(file_size).encode())
        
        # 3. Client waits for quota check result
        response = sock.recv(1024).decode()
        if response == "Insufficient quota":
            print("Insufficient quota to upload the file.")
            return False
        elif response != "QUOTA_OK":
            print(f"Unexpected server response regarding quota: {response}")
            return False
        
        # 4. Client sends file data
        with open(filepath, 'rb') as f:
            while True:
                data = f.read(1024)
                if not data:
                    break
                sock.sendall(data)
        
        # 5. Client waits for server's final confirmation
        final_upload_response = sock.recv(1024).decode()
        print(final_upload_response)
        return True 
    except socket.error as e:
        print(f"Socket error during file send: {e}")
        return False
    except Exception as e:
        print(f"Error during file send: {e}")
        return False

def receive_file(sock, filepath, file_size):
    """Handles receiving a file from the server, reading exactly file_size bytes."""
    try:
        received_bytes = 0
        # Ensure the directory for the file exists
        os.makedirs(os.path.dirname(filepath) or '.', exist_ok=True)
        
        with open(filepath, 'wb') as f:
            while received_bytes < file_size:
                bytes_to_receive = min(1024, file_size - received_bytes)
                if bytes_to_receive == 0: 
                    break # Should not happen if file_size is correct
                data = sock.recv(bytes_to_receive)
                if not data: # Server disconnected or error during transfer
                    print(f"Error: Server disconnected during download of '{os.path.basename(filepath)}'. Incomplete file.")
                    # Optionally clean up incomplete file: os.remove(filepath)
                    return False
                f.write(data)
                received_bytes += len(data)
        
        if received_bytes == file_size:
            print(f"Successfully downloaded '{os.path.basename(filepath)}' ({received_bytes} bytes).")
            return True
        else:
            print(f"Error: Incomplete download for '{os.path.basename(filepath)}'. Expected {file_size}, received {received_bytes}.")
            # Optionally clean up incomplete file: os.remove(filepath)
            return False
    except socket.error as e:
        print(f"Socket error during file reception: {e}")
        return False
    except Exception as e:
        print(f"Error during file reception: {e}")
        return False

# Main client loop
sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
try:
    sock.connect((HOST, PORT))
    print(f"Connected to server at {HOST}:{PORT}")
except socket.error as e:
    print(f"Failed to connect to server: {e}. Ensure the server is running.")
    exit()

authenticated = False
while not authenticated:
    action = input("Enter 'login' to log in or 'register' to create a new account: ").strip().lower()
    if action == 'login' or action == 'register':
        username = input("Username: ").strip()
        password = input("Password: ").strip()
        if not username or not password:
            print("Username and password cannot be empty.")
            continue
        request = f"{action} {username} {password}"
        try:
            sock.send(request.encode())
            response = sock.recv(1024).decode()
            print(response)
            if response == "Authenticated" or response == "Registered":
                authenticated = True
            elif "Bad request" in response:
                print("Server responded with Bad request. Please check format.")
        except socket.error as e:
            print(f"Socket error during authentication: {e}. Connection lost.")
            break # Exit if connection fails
    else:
        print("Invalid action. Please enter 'login' or 'register'.")

# If authentication fails or connection drops, exit.
if not authenticated:
    sock.close()
    exit()

# Commands explanation
print("\n--- Available Commands ---")
print("ls [path]                - List directory contents")
print("pwd                      - Print current 'working' directory (confined view)")
print("mkdir <dirname>          - Create a new directory")
print("rmdir <dirname>          - Remove a directory (recursively deletes contents)")
print("rmfile <filename>        - Remove a file")
print("rename <oldname> <newname> - Rename a file or directory")
print("copy <source> <destination> - Copy a file or directory within your space")
print("upload <local_file>      - Upload a file from your computer to server")
print("download <remote_file>   - Download a file from server to your computer")
print("exit                     - Disconnect from the server")
print("stop                     - Stop the server (admin only)")
print("--------------------------")

while True:
    try:
        request = input('> ').strip()
        if not request: 
            request = "pwd" # Default if empty input

        command_parts = request.split()
        command = command_parts[0].lower() if command_parts else ""

        if command == 'upload':
            if len(command_parts) < 2:
                print("Usage: upload <local_filepath>")
                continue
            
            local_filepath = command_parts[1]
            if os.path.exists(local_filepath) and os.path.isfile(local_filepath):
                # Send the upload command itself, then handle handshake in send_file
                sock.send(request.encode()) 
                send_file(sock, local_filepath)
            else:
                print(f"Error: Local file '{local_filepath}' does not exist or is not a file.")
                continue # Do not send anything to server if local file is invalid

        elif command == 'download':
            if len(command_parts) < 2:
                print("Usage: download <remote_filename>")
                continue
            
            remote_filename = command_parts[1]
            sock.send(request.encode()) # Send the download command
            
            # Client waits for server's DOWNLOAD_READY response with file size
            response_from_server = sock.recv(1024).decode()
            if response_from_server.startswith('DOWNLOAD_READY'):
                try:
                    file_size = int(response_from_server.split()[1])
                    # Determine local path for downloaded file (e.g., in current working dir)
                    local_download_path = os.path.join(os.getcwd(), os.path.basename(remote_filename))
                    receive_file(sock, local_download_path, file_size)
                except (ValueError, IndexError) as e:
                    print(f"Error parsing download size from server: {response_from_server} ({e})")
            else:
                print(response_from_server) # This would be "File does not exist" or an error message

        elif command == 'copy':
            if len(command_parts) < 3:
                print("Usage: copy <source_file_or_dir> <destination_file_or_dir>")
                continue
            sock.send(request.encode())
            response = sock.recv(4096).decode() # Increased buffer for potentially long error messages
            print(response)

        else: # For other commands (pwd, ls, mkdir, rmdir, rmfile, rename, exit, stop)
            if not command_parts: # Should be caught by `if not request` but for safety
                continue

            sock.send(request.encode())
            response = sock.recv(4096).decode() # Increased buffer for potentially long error messages
            print(response)
            
            if response == 'exit' or response == 'Server stopping':
                break

    except socket.error as e:
        print(f"Socket error: {e}. Connection closed.")
        break
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        break

sock.close()
print("Client disconnected.")