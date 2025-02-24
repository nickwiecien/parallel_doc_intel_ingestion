import azure.functions as func
import azure.durable_functions as df
import logging
import json
import os
import hashlib
from azure.storage.blob import BlobServiceClient
from azure.cosmos import CosmosClient
from azure.identity import DefaultAzureCredential
from pypdf import PdfReader, PdfWriter
import pikepdf
from io import BytesIO
from datetime import datetime
import filetype
import fitz as pymupdf
from PIL import Image
import io
import base64
import random
import pandas as pd
import tempfile
import subprocess


app = df.DFApp(http_auth_level=func.AuthLevel.FUNCTION)

from doc_intelligence_utilities import analyze_pdf, extract_results

# An HTTP-Triggered Function with a Durable Functions Client binding
@app.route(route="orchestrators/{functionName}")
@app.durable_client_input(client_name="client")
async def http_start(req: func.HttpRequest, client):
    function_name = req.route_params.get('functionName')
    payload = json.loads(req.get_body())

    instance_id = await client.start_new(function_name, client_input=payload)
    response = client.create_check_status_response(req, instance_id)
    return response

# Orchestrators
@app.orchestration_trigger(context_name="context")
def document_extraction_orchestrator(context):

    first_retry_interval_in_milliseconds = 5000
    max_number_of_attempts = 2
    retry_options = df.RetryOptions(first_retry_interval_in_milliseconds, max_number_of_attempts)

    # Get the input payload from the context
    payload = context.get_input()
    
    # Extract the container names from the payload
    source_container = payload.get("source_container")
    prefix_path = payload.get("prefix_path")

    pages_container = f'{source_container}-pages'
    doc_intel_results_container = f'{source_container}-doc-intel-results'
    doc_intel_formatted_results_container = f'{source_container}-doc-intel-formatted-results'


    # Confirm that all storage locations exist to support document ingestion
    try:
        container_check = yield context.call_activity_with_retry("check_containers", retry_options, json.dumps({'source_container': source_container}))
        context.set_custom_status('Document Processing Containers Checked')
        
    except Exception as e:
        context.set_custom_status('Extraction Failed During Container Check')
        logging.error(e)
        raise e


    try:
        files = yield context.call_activity_with_retry("get_source_files", retry_options, json.dumps({'source_container': source_container, 'extensions': ['.doc', '.docx', '.dot', '.dotx', '.odt', '.ott', '.fodt', '.sxw', '.stw', '.uot', '.rtf', '.txt', '.xls', '.xlsx', '.xlsm', '.xlt', '.xltx', '.ods', '.ots', '.fods', '.sxc', '.stc', '.uos', '.csv', '.ppt', '.pptx', '.pps', '.ppsx', '.pot', '.potx', '.odp', '.otp', '.fodp', '.sxi', '.sti', '.uop', '.odg', '.otg', '.fodg', '.sxd', '.std', '.svg', '.html', '.htm', '.xps', '.epub', '.pdf', '.vtt','.mp3', '.mp4', '.mpweg', '.mpga', '.m4a', '.wav', '.webm' ], 'prefix': prefix_path}))
        context.set_custom_status('Retrieved Source Files')
    except Exception as e:
        context.set_custom_status('Extraction Failed During File Retrieval')
        logging.error(e)
        raise e
    
    # Initialize lists to store parent and extracted files
    parent_files = []
    extracted_files = []
    
    # For each PDF file, split it into single-page chunks and save to pages container
    try:
        split_pdf_tasks = []
        for file in files:
            # Append the file to the parent_files list
            parent_files.append(file)
            # Create a task to split the PDF file and append it to the split_pdf_tasks list
            split_pdf_tasks.append(context.call_activity_with_retry("split_pdf_files", retry_options, json.dumps({'source_container': source_container, 'pages_container': pages_container, 'file': file})))
        # Execute all the split PDF tasks and get the results
        split_pdf_files = yield context.task_all(split_pdf_tasks)
        # Flatten the list of split PDF files
        split_pdf_files = [item for sublist in split_pdf_files for item in sublist]

        # Convert the split PDF files from JSON strings to Python dictionaries
        pdf_pages = [json.loads(x) for x in split_pdf_files]

    except Exception as e:
        context.set_custom_status('Ingestion Failed During PDF Splitting')
        logging.error(e)
        raise e

    context.set_custom_status('PDF Splitting Completed')

    # For each PDF page, process it with Document Intelligence and save the results to the document intelligence results (and formatted results) container
    try:
        extract_pdf_tasks = []
        for pdf in pdf_pages:
            # Append the child file to the extracted_files list
            extracted_files.append(pdf['child'])
            # Create a task to process the PDF page and append it to the extract_pdf_tasks list
            extract_pdf_tasks.append(context.call_activity("process_pdf_with_document_intelligence", json.dumps({'child': pdf['child'], 'parent': pdf['parent'], 'pages_container': pages_container, 'doc_intel_results_container': doc_intel_results_container, 'doc_intel_formatted_results_container': doc_intel_formatted_results_container})))
        # Execute all the extract PDF tasks and get the results
        extracted_pdf_files = yield context.task_all(extract_pdf_tasks)

    except Exception as e:
        context.set_custom_status('Ingestion Failed During Document Intelligence Extraction')
        logging.error(e)
        raise e

    context.set_custom_status('Document Extraction Completion')

@app.activity_trigger(input_name="activitypayload")
def check_containers(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    
    # Extract the source container, file extension, and prefix from the payload
    source_container = data.get("source_container")
    
    pages_container = f'{source_container}-pages'
    doc_intel_results_container = f'{source_container}-doc-intel-results'
    doc_intel_formatted_results_container = f'{source_container}-doc-intel-formatted-results'
    image_analysis_results_container = f'{source_container}-image-analysis-results'
    
    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    try:
        blob_service_client.create_container(doc_intel_results_container)
    except Exception as e:
        pass

    try:
        blob_service_client.create_container(pages_container)
    except Exception as e:
        pass

    try:
        blob_service_client.create_container(image_analysis_results_container)
    except Exception as e:
        pass

    try:
        blob_service_client.create_container(doc_intel_formatted_results_container)
    except Exception as e:
        pass

    # Return the list of file names
    return True

@app.activity_trigger(input_name="activitypayload")
def split_pdf_files(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    
    # Extract the source container, chunks container, and file name from the payload
    source_container = data.get("source_container")
    pages_container = data.get("pages_container")
    file = data.get("file")

    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])
    
    # Get a ContainerClient object for the source and chunks containers
    source_container = blob_service_client.get_container_client(source_container)
    pages_container = blob_service_client.get_container_client(pages_container)

    # Get a BlobClient object for the PDF file
    pdf_blob_client = source_container.get_blob_client(file)

    # Initialize an empty list to store the PDF chunks
    pdf_chunks = []

    # If the PDF file exists
    if  pdf_blob_client.exists():

        blob_data = pdf_blob_client.download_blob().readall()

        kind = filetype.guess(blob_data)

        if kind.EXTENSION != 'pdf':
            raise Exception(f'{file} is not of type PDF. Detected MIME type: {kind.EXTENSION}')

        # Create a PdfReader object for the PDF file
        pdf_reader = pikepdf.open(BytesIO(blob_data))

        # Get the number of pages in the PDF file
        num_pages = len(pdf_reader.pages)

        # For each page in the PDF file
        for i in range(num_pages):
            # Create a new file name for the PDF chunk
            new_file_name = file.replace('.pdf', '') + '_page_' + str(i+1) + '.pdf'

            new_pdf = pikepdf.Pdf.new()

            # Create a PdfWriter object
            # pdf_writer = PdfWriter()
            # Add the page to the PdfWriter object
            new_pdf.pages.append(pdf_reader.pages[i])
            # pdf_writer.add_page(pdf_reader.pages[i])

            # Create a BytesIO object for the output stream
            output_stream = BytesIO()
            # Write the PdfWriter object to the output stream
            new_pdf.save(output_stream)

            # Reset the position of the output stream to the beginning
            output_stream.seek(0)

            # Get a BlobClient object for the PDF chunk
            pdf_chunk_blob_client = pages_container.get_blob_client(blob=new_file_name)

            # Upload the PDF chunk to the chunks container
            pdf_chunk_blob_client.upload_blob(output_stream, overwrite=True)
            
            # Append the parent file name and child file name to the pdf_chunks list
            pdf_chunks.append(json.dumps({'parent': file, 'child': new_file_name}))

    # Return the list of PDF chunks
    return pdf_chunks
    
@app.activity_trigger(input_name="activitypayload")
def process_pdf_with_document_intelligence(activitypayload: str):
    """
    Process a PDF file using Document Intelligence.

    Args:
        activitypayload (str): The payload containing information about the PDF file.

    Returns:
        str: The updated filename of the processed PDF file.
    """

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)

    # Extract the child file name, parent file name, and container names from the payload
    child = data.get("child")
    parent = data.get("parent")
    pages_container = data.get("pages_container")
    doc_intel_results_container = data.get("doc_intel_results_container")
    doc_intel_formatted_results_container = data.get("doc_intel_formatted_results_container")

    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])

    # Get a ContainerClient object for the pages, Document Intelligence results, and DI formatted results containers
    pages_container_client = blob_service_client.get_container_client(container=pages_container)
    doc_intel_results_container_client = blob_service_client.get_container_client(container=doc_intel_results_container)
    doc_intel_formatted_results_container_client = blob_service_client.get_container_client(container=doc_intel_formatted_results_container)

    # Get a BlobClient object for the PDF file
    pdf_blob_client = pages_container_client.get_blob_client(blob=child)

    # Initialize a flag to indicate whether the PDF file has been processed
    processed = False

    # Create a new file name for the processed PDF file
    updated_filename = child.replace('.pdf', '.json')

    # Get a BlobClient object for the Document Intelligence results file
    doc_results_blob_client = doc_intel_results_container_client.get_blob_client(blob=updated_filename)
    # Check if the Document Intelligence results file exists
    if doc_results_blob_client.exists():

        # Get a BlobClient object for the extracts file
        extract_blob_client = doc_intel_formatted_results_container_client.get_blob_client(blob=updated_filename)

        # If the extracts file exists
        if extract_blob_client.exists():

            # Download the PDF file as a stream
            pdf_stream_downloader = (pdf_blob_client.download_blob())

            # Calculate the MD5 hash of the PDF file
            md5_hash = hashlib.md5()
            for byte_block in iter(lambda: pdf_stream_downloader.read(4096), b""):
                md5_hash.update(byte_block)
            checksum = md5_hash.hexdigest()

            # Load the extracts file as a JSON string
            extract_data = json.loads((extract_blob_client.download_blob().readall()).decode('utf-8'))

            # If the checksum in the extracts file matches the checksum of the PDF file
            if 'checksum' in extract_data.keys():
                if extract_data['checksum']==checksum:
                    # Set the processed flag to True
                    processed = True

    # If the PDF file has not been processed
    if not processed:
        # Extract the PDF file with AFR, save the AFR results, and save the extract results

        # Download the PDF file
        pdf_data = pdf_blob_client.download_blob().readall()
        # Analyze the PDF file with Document Intelligence
        doc_intel_result = analyze_pdf(pdf_data)

        # Get a BlobClient object for the Document Intelligence results file
        doc_intel_result_client = doc_intel_results_container_client.get_blob_client(updated_filename)

        # Upload the Document Intelligence results to the Document Intelligence results container
        doc_intel_result_client.upload_blob(json.dumps(doc_intel_result), overwrite=True)

        # Extract the results from the Document Intelligence results
        page_map = extract_results(doc_intel_result, updated_filename)

        # Extract the page number from the child file name
        page_number = child.split('_')[-1]
        page_number = page_number.replace('.pdf', '')
        # Get the content from the page map
        content = page_map[0][1]

        # Generate a unique ID for the record
        id_str = child 
        hash_object = hashlib.sha256()
        hash_object.update(id_str.encode('utf-8'))
        id = hash_object.hexdigest()

        # Download the PDF file as a stream
        pdf_stream_downloader = (pdf_blob_client.download_blob())

        # Calculate the MD5 hash of the PDF file
        md5_hash = hashlib.md5()
        for byte_block in iter(lambda: pdf_stream_downloader.read(4096), b""):
            md5_hash.update(byte_block)
        checksum = md5_hash.hexdigest()

        # Create a record for the PDF file
        record = {
            'content': content,
            'sourcefile': parent,
            'sourcepage': child,
            'pagenumber': page_number,
            'category': 'manual',
            'id': str(id),
            'checksum': checksum
        }

        # Get a BlobClient object for the extracts file
        extract_blob_client = doc_intel_formatted_results_container_client.get_blob_client(blob=updated_filename)

        # Upload the record to the extracts container
        extract_blob_client.upload_blob(json.dumps(record), overwrite=True)

    # Return the updated file name
    return updated_filename

@app.activity_trigger(input_name="activitypayload")
def get_source_files(activitypayload: str):

    # Load the activity payload as a JSON string
    data = json.loads(activitypayload)
    
    # Extract the source container, file extension, and prefix from the payload
    source_container = data.get("source_container")
    extensions = data.get("extensions")
    prefix = data.get("prefix")
    
    # Create a BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(os.environ['STORAGE_CONN_STR'])
    
    try:
        # Get a ContainerClient object from the BlobServiceClient
        container_client = blob_service_client.get_container_client(source_container)
        # List all blobs in the container that start with the specified prefix
        blobs = container_client.list_blobs(name_starts_with=prefix)

    except Exception as e:
        # If the container does not exist, return an empty list
        return []

    if not container_client.exists():
        return []
    
    # Initialize an empty list to store the names of the files
    files = []

    # For each blob in the container
    for blob in blobs:
        # If the blob's name ends with the specified extension
        if '.' + blob.name.lower().split('.')[-1] in extensions:
            # Append the blob's name to the files list
            files.append(blob.name)

    # Return the list of file names
    return files