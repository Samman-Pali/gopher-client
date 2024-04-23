# COMP3310 Assignment 2 - 2024 Semester 1 | Australian National University
# @AUTHOR: U7287889 - Samman Palihapitiya

import socket
import os
import sys
import time
from typing import Deque, Dict, List, Set, Union, Optional, IO, Any
import datetime
from collections import deque
import textwrap

# ANSI codes for colours and styles
OKBLUE: str = "\033[94m"
OKCYAN: str = "\033[96m"
OKGREEN: str = "\033[92m"
WARNING: str = "\033[93m"
FAIL: str = "\033[91m"
ENDC: str = "\033[0m"
BOLD: str = "\033[1m"
LINEUP: str = "\033[A\033[A"
UNDERLINE: str = "\033[4m"

HOST: str = "comp3310.ddns.net"
PORT: int = 70
COURSE_TAG: str = "comp3310"
CRLF: str = b"\r\n"

# Item type characters - https://www.rfc-editor.org/rfc/rfc1436
TXTFILE: str = "0"
DIRECTORY: str = "1"
ERROR: str = "3"
BINARY: str = "9"  # must read until TCP closes - no period at EOF (.xxx extension?)
INFORMATION: str = "i"

# doubly ended queue is efficient w popping items off
DIR_TO_VISIT: Deque[str] = deque()
# keep track of directories that have been searched
DIRS_VISITED: Dict[str, str] = {}

# will capture text, binary and image files from gopher server
GOPHER_RESOURCES: List[Dict[str, Union[str, Optional[int], str]]] = []

# error handling sets - helps deduplication
INVAL_REFS: Set[str] = set()
EX_REFS: Set[str] = set()

# informational message in the server
INFO_MESSAGE: List[str] = []

FILE_SIZES: Dict[str, Optional[Dict[str, Union[str, int, None]]]] = {
    "smallest_text_file": None,
    "largest_text_file": None,
    "smallest_binary_file": None,
    "largest_binary_file": None,
}


def connect_to_server(host: str, port: int) -> socket.socket:
    """
    Establishes the TCP connection to the specified host and port.
    Return an active socket object that is connected to server, however,
    on fail, an exception is raised with details of the failure.
    """

    socket_obj = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        socket_obj.connect((host, port))
    except Exception as e:
        print(f"Error connecting to {host} on port {port}: {e}")
        socket_obj.close()
        raise
    return socket_obj


def send_request(selector: str, host: str, port: int) -> IO[Any]:

    req_attempts = 0
    retries = 2

    while req_attempts < retries:
        try:
            socket = connect_to_server(host, port)
            # debugging print
            print(
                selector,
                "requested from",
                host,
                "on port",
                port,
                "-",
                datetime.datetime.now(),
            )
            # create request with CRLF and send thru socket
            request = selector + CRLF
            socket.send(request)

            file = socket.makefile()
            socket.close()
            # response is returned as file
            return file
        except Exception as e:
            # if request failed, try again - final attempt
            print(
                f"{WARNING}Request attempt {req_attempts+1} failed with error: {e}{ENDC}"
            )
            req_attempts += 1
    raise Exception(f"{FAIL}Max number of attempts reached, request failed.{ENDC}")


# given selector, item type, host and port, will return a string of the url to the resource
# the returned url can be placed in the Floodgap url search bar for navigation
def create_gopher_url(selector: str, item_type: str, host: str, port: int = 70) -> str:
    return f"gopher://{OKBLUE}{host}{ENDC}:{port}/{item_type}{OKCYAN}{selector}{ENDC}"


# helps keep track of the amount of data downlaoded
def progress_bar(progress: int):
    sys.stdout.write(f"\r{OKGREEN}Downloaded: {progress} bytes{ENDC}")
    sys.stdout.flush()


# used for the final print formatting to avoid overly long lines
def print_wrapped(text: str, width: int):
    wrapper = textwrap.TextWrapper(width=width)
    wrapped_text = wrapper.fill(text)
    print(wrapped_text)


def handle_ex_refs(line: str) -> None:
    """
    This function processes external references and malformed lines returned by the gopher server. Primary
    purpose in this case is to identify external references - i.e., pointers outside the comp3310 domain.
    """
    item_type = line[0]
    rest_of_line = line[1:]
    parts = rest_of_line.split("\t")

    if len(parts) == 4:
        _, _, host, port = parts
        message = f"Host: {OKBLUE}{host}{ENDC}, Port: {OKBLUE}{port}{ENDC}"
        EX_REFS.add(message)
    else:
        message = f"Potentially Malformed Line: {WARNING}{rest_of_line}{ENDC}"
        EX_REFS.add(message)

    return None


def handle_inval_refs(line: str, selector: str) -> None:
    # records invalid references from gopher server lines, item type 3 issues.
    line = line[1:]  # exclude the type number
    message = f"Invalid Reference from {OKCYAN}{selector}{ENDC}: {WARNING}{line}{ENDC}"
    INVAL_REFS.add(message)
    return None


def parse_file(line: str, file_type: str) -> None:
    """
    Processes a line from the gopher menu that corresponds to a file (text/binary), downloads this file
    and updates the list of resources for printing.
    """

    # extracting the file's directory from the line
    start_index = line.find("/")
    file_directory = line[start_index:]
    end_index = file_directory.find(COURSE_TAG)
    file_directory = file_directory[:end_index]

    # download the file and get its size
    size = download_file(file_directory, file_type)

    if size is None:
        print(
            f"{LINEUP}{FAIL}File excluded, Skipping addition to resources: {file_directory}{ENDC}{ENDC}\n"
        )
        return

    # removes the trailing /t following selector/path
    inval_index = file_directory.find("\t")
    file_directory = file_directory[:inval_index]

    # create the URL for accessing the file
    url = create_gopher_url(file_directory, line[0], HOST, PORT)
    file_resource = {"url": url, "size": size, "type": line[0]}

    # append unique resources to the list
    if file_resource not in GOPHER_RESOURCES:
        GOPHER_RESOURCES.append(file_resource)

    return None


def parse_menu(file: IO[Any], selector: str = "") -> None:
    """
    Prases the content of a gopher directory listing, extracting information about direcotires, text and binary files.
    Able to handle external references and error/invalid references. It updates global structures for directories to visit as well as
    for resources found.

    Each line from the given menu is analysed to determine its type - directory, text, binary etc. - appropriate action is taken based
    on this. Directories get added to a queue for visting while text/binary files get downloaded for metadata analysis.
    """

    global DIR_TO_VISIT, DIRS_VISITED  # global lists get modified in here
    for line in file:
        line = line.strip()  # strip white space at front
        item_type = line[0]  # gopher item type is the first char

        # process directories
        if item_type == DIRECTORY:

            # process external references by looking for hosts and ports outside - tacky but works for this
            if HOST not in line or str(PORT) not in line:
                handle_ex_refs(line)

            start_index = line.find("/")
            if (
                start_index != -1
            ):  # if fulfilled, likely to have found a path...continue processing
                # note the end of directory path is denoted by the tab '\t'
                end_index = line.find("\t", start_index)
                # if the tab character is found, then we likely have complete path
                if end_index != -1:
                    dir_path = line[start_index:end_index]
                else:
                    dir_path = line[start_index:]

                # add new path to directories to visit if it hasn't already been visited or is to be visited
                if (
                    dir_path
                    and (dir_path not in DIRS_VISITED)
                    and (dir_path not in DIR_TO_VISIT)
                ):
                    DIR_TO_VISIT.append(
                        dir_path
                    )  # new directory path added to visit list

        # process text files
        elif item_type == TXTFILE:
            parse_file(line, "text")

        # process binaries
        elif item_type == BINARY:

            file_type = "image" if "jpeg" in line else "binary"
            parse_file(line, file_type)

        # process error lines
        elif item_type == ERROR:
            handle_inval_refs(line, selector)

        else:
            # handle information text, assumption: 'i' prefix in lines refer to information
            if item_type == INFORMATION:
                info_text = line[1:].strip()
                if not info_text.startswith("invalid"):
                    message = f"{OKBLUE}Information:{ENDC} {OKGREEN}{selector}{ENDC} {info_text}"
                    INFO_MESSAGE.append(message)
            else:
                continue
                # TODO: if server contains other resource types, handle appropriately. Out of scope.
    file.close()
    return None


def download_file(
    selector: str, file_type: str, max_size: int = 500000, timeout: int = 5
) -> int:
    """
    Downloads a file from the gopher server. Able to handle requests for download of jpegs, binaries and text files.
    Max file size limited to 0.5MB to prevent extremely large file downloads, timeout set to 5 seconds to avoid lengthy hangs.
    """

    print(
        "Retrieving",
        selector,
        "from",
        HOST,
        "on port",
        PORT,
        "-",
        datetime.datetime.now(),
    )

    # create request, socket/connection and set socket timeout
    idx = selector.find("\\")
    selector = selector[:idx]
    request = str.encode(selector) + CRLF
    socket_obj = connect_to_server(HOST, PORT)
    socket_obj.settimeout(5)
    socket_obj.send(request)

    # receive reponse of the file
    response = b""
    bytes_received = 0
    start_time = time.time()

    try:
        while bytes_received < max_size:
            if time.time() - start_time > timeout:
                raise TimeoutError

            file_components = socket_obj.recv(2048)
            if not file_components:
                break

            response += file_components
            bytes_received += len(file_components)
            progress_bar(bytes_received)

            if bytes_received >= max_size:
                print(
                    f"\n{WARNING}Max file size reached - file capped at {max_size/1000000}MB{ENDC}"
                )
                return None

    except Exception as e:
        if TimeoutError:
            print(f"{WARNING}\nTimed out while retrieving {selector}.{ENDC}")
        if bytes_received == 0:
            print(f"{WARNING}Sorry, no data received for {selector}. \U0001F641{ENDC}")

        return None
    finally:
        print("\n")
        socket_obj.close()

    # handles issues to do with txtfile termination, gopher server
    # seems to add its own newline following the text content then CRLF
    if file_type in ["text", "binary"]:
        text_response = response.decode("utf-8")
        cleaned_response = text_response.rstrip("\n.\r\n")
        response = cleaned_response.encode("utf-8")

    file_extension_map = {"text": ".txt", "binary": ".dat", "image": ".jpeg"}
    extension = file_extension_map.get(file_type, "_")

    # file name capped to 50 characters
    filename = f"{file_type}files{selector}"
    if len(filename) > 50:
        filename = filename[:50]

    # only add file extension tag if its missing
    if not filename.lower().endswith(extension):
        filename += extension

    filepath = os.path.join("gopher-downloads", filename)
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    with open(filepath, "wb") as file:
        file.write(response)

    file_size = os.path.getsize(filepath)
    return file_size if file_size < max_size else None


def find_largest_and_smallest_files() -> None:
    """
    Identifies the largest and smallest text and binary files that were downloaded.
    It excludes errored/timed out downloads by filtering sizes of None.
    """
    global FILE_SIZES
    # float inf is positive infinite, therefore, any number we find will reset it
    smallest_text_size = float("inf")
    smallest_binary_size = float("inf")
    # -1 size initiation so first file will overwrite, then follow-on
    largest_text_size = -1
    largest_binary_size = -1

    for resource in GOPHER_RESOURCES:
        # ensures resource has valid size and not None, meaning it wasn't errored or timed out.
        if "size" in resource and resource["size"] is not None:
            if resource["type"] == TXTFILE:
                # find smallest text file size
                if resource["size"] < smallest_text_size:
                    smallest_text_size = resource["size"]
                    FILE_SIZES["smallest_text_file"] = resource
                # find largest text file size
                if resource["size"] > largest_text_size:
                    largest_text_size = resource["size"]
                    FILE_SIZES["largest_text_file"] = resource
            elif resource["type"] == BINARY:
                # find smallest binary file
                if resource["size"] < smallest_binary_size:
                    smallest_binary_size = resource["size"]
                    FILE_SIZES["smallest_binary_file"] = resource
                # find largest binary file
                if resource["size"] > largest_binary_size:
                    largest_binary_size = resource["size"]
                    FILE_SIZES["largest_binary_file"] = resource
    return None


def web_crawler() -> None:
    """
    Crawls the gopher server, starting from the root directory, to discover and process resources.
    """
    global DIRS_VISITED

    # iterate through all directories in gopher server until no more directories left to visit
    while DIR_TO_VISIT:

        current_dir = DIR_TO_VISIT.popleft()
        if current_dir not in DIRS_VISITED:
            # we know these are directories, so I've set the item type
            url = create_gopher_url(current_dir, "1", HOST, PORT)
            DIRS_VISITED[current_dir] = url

            selector = str.encode(current_dir)
            # grab page content
            web_page = send_request(selector, HOST, PORT)
            # perform processing - image, text, binary, directory
            parse_menu(web_page, selector)
    return None


def main() -> None:
    # start from root directory - b''
    web_page = send_request(b"", HOST, PORT)
    parse_menu(web_page, "")
    # crawl gopher server
    web_crawler()
    # determine largest/smallest resources
    find_largest_and_smallest_files()
    return None


if __name__ == "__main__":
    main()

    text_files = [
        resource for resource in GOPHER_RESOURCES if resource["type"] == TXTFILE
    ]
    binary_files = [
        resource for resource in GOPHER_RESOURCES if resource["type"] == BINARY
    ]

    print(BOLD + "=" * 130)
    print(BOLD + UNDERLINE + "Directory and File Count Information:" + ENDC)
    print(f"{OKGREEN}Number of Directories:{ENDC} {FAIL}{len(DIRS_VISITED)}{ENDC}")

    print(BOLD + "-" * 130)
    print(OKGREEN + f"Number of External References:{ENDC} {FAIL}{len(EX_REFS)}" + ENDC)
    for i, ref in enumerate(sorted(EX_REFS), start=1):
        print(f"\t{i}) {ref}")

    print(BOLD + "-" * 130)
    print(
        OKGREEN
        + f"Number of Invalid References (Error Types):{ENDC} {FAIL}{len(INVAL_REFS)}"
        + ENDC
    )
    for j, ref in enumerate(sorted(INVAL_REFS), start=1):
        print(f"\t{j}) {ref}")

    print(BOLD + "-" * 130)
    print(OKGREEN + f"Number of Text Files:{ENDC} {FAIL}{len(text_files)}" + ENDC)
    for i, file in enumerate(text_files, start=1):
        print_wrapped(f"\t{i}) {file['url']}", 120)

    print(BOLD + "-" * 130)
    print(OKGREEN + f"Number of Binary Files:{ENDC} {FAIL}{len(binary_files)}" + ENDC)
    for i, file in enumerate(binary_files, start=1):
        print(f"\t{i}) {file['url']}")

    print(BOLD + "=" * 130)
    print(BOLD + UNDERLINE + "File Size Information:" + ENDC)
    for category, file_info in FILE_SIZES.items():
        if file_info:
            print(
                f"{category.replace('_', ' ').title()}: {file_info['url']} {OKGREEN}(Size: {file_info['size']} bytes){ENDC}"
            )
        else:
            print(f"{category.replace('_', ' ').title()}: None")

    print(BOLD + "=" * 130)

    print(BOLD + UNDERLINE + "Informational Messages:" + ENDC)
    for message in INFO_MESSAGE:
        print(message)
    print(BOLD + "=" * 130)
