# Document Processing with Azure Durable Functions

This project is an Azure Durable Functions application designed to facilitate large-scale document processing. It handles OCR and image analysis (using GPT-4o) by splitting PDFs into individual pages and processing each page using Document Intelligence. The solution can run locally during development or be deployed to Azure for production workloads.

## Overview

This Durable Functions project orchestrates the following tasks:
- **File Retrieval:** Lists documents from a specified source location.
- **PDF Splitting:** Splits multi-page PDF documents into individual page files.
- **Document Intelligence Processing:** Processes each PDF page to extract content using OCR.
- **Image Analysis:** Analyzes pages for embedded visuals and produces detailed descriptions where applicable.

The system is designed to scale and is ideal for processing large batches of documents using cloud resources.

## Features

- **Durable Orchestration:** Coordinates multiple activity functions to process documents in a fault-tolerant manner.
- **PDF Splitting:** Uses `pikepdf` and other libraries to split PDFs into single-page documents.
- **Document Intelligence Integration:** Processes each page to extract textual content and metadata.
- **Image Analysis:** Leverages external utilities to analyze visual content (integrated via GPT-4o).

## Prerequisites

- **Azure Functions Core Tools:** For local development and testing.
- **Python 3.8+**
- **Azure Subscription:** Required for deploying and using Azure resources.
- **Required Python Packages:**
  - `azure-functions`
  - `azure-durable-functions`
  - `azure-storage-blob`
  - `azure-cosmos`
  - `azure-identity`
  - `pypdf`, `pikepdf`
  - `filetype`
  - `fitz` (PyMuPDF)
  - `Pillow`
  - `pandas`
  - (and any additional dependencies required by your project)

## Setup

1. **Clone the Repository:**

   ```bash
   git clone <repository_url>
   cd <repository_directory>
   ```

2. **Create a Virtual Environment and Install Dependencies:**

   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Configure Environment Variables:**

   Create or update your `local.settings.json` file with the necessary environment variables. For example:

   ```json
   {
     "IsEncrypted": false,
     "Values": {
       "AzureWebJobsStorage": "<your_storage_connection_string>", # See documentation for local development
       "FUNCTIONS_WORKER_RUNTIME": "python",
       "STORAGE_CONN_STR": "<your_storage_connection_string>" # Needed for retrieving files from cloud storage location
     }
   }
   ```

## Local Development

To run the project locally, use the Azure Functions Core Tools:

```bash
func start
```

### HTTP Trigger (`http_start`)

Exposes an endpoint to initiate the orchestration process. When invoked via an HTTP POST request, it starts a new orchestration instance using the provided payload.

**Example Request:**

```http
POST /api/orchestrators/<functionName> HTTP/1.1
Host: localhost:7071
Content-Type: application/json

{
  "source_container": "your-source-container",
  "prefix_path": "optional-prefix"
}
```

### Orchestrator (`document_extraction_orchestrator`)

Coordinates the overall document processing flow including:
- Checking for necessary storage containers.
- Retrieving source files.
- Splitting PDF files into individual pages.
- Processing each page using Document Intelligence.
- Optionally analyzing images for additional visual content.

---

### Activity Functions

- **`check_containers`:**  
  Creates required blob containers if they do not exist.

- **`get_source_files`:**  
  Retrieves files from the source container based on specified extensions.

- **`split_pdf_files`:**  
  Splits a multi-page PDF into single-page PDF files.

- **`process_pdf_with_document_intelligence`:**  
  Processes a PDF page to extract textual content, calculates file checksums, and stores results.

---

### Utility Functions

Functions imported from `doc_intelligence_utilities` and `aoai_utilities` handle document analysis and image classification.


