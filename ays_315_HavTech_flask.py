##the file path for this application is C:\Users\joeyf\Music\pdf reader rom\mccook_deploy_1
from flask import Flask, request, jsonify, render_template, url_for, send_from_directory, redirect, send_file, session, flash
from flask_cors import CORS
import os
import spacy
import pandas as pd
import re
import logging
from werkzeug.utils import secure_filename
from PyPDF2 import PdfReader, PdfWriter
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
import threading
import fitz  # PyMuPDF
from dotenv import load_dotenv
import openai  # Add this for OpenAI API integration
from pathlib import Path
import json  # Import JSON to serialize data for the session
from ays_314_script import process_pdf_file  # Import the external script
import base64
import tempfile
import io
import os
import io
import base64
import smtplib
import logging
import pandas as pd
from flask import Flask, request, jsonify, after_this_request
from werkzeug.utils import secure_filename
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText
from datetime import datetime
import os
import pandas as pd
import logging
from collections import Counter
from openpyxl import load_workbook
from openpyxl.styles import numbers
import shutil
import boto3
from helpers_async_s3_0_3 import (
    AWS_REGION, S3_BUCKET, S3_UPLOAD_PREFIX, S3_RESULTS_PREFIX,
    s3_key, s3_upload_bytes, s3_upload_file, s3_presign_get,
    slugify, make_project_id,
    submit_job, set_job, get_job,
    run_pipeline_to_s3,
    DASHBOARD_XLSX,
    log_completed_job_row,
    CUSTOMER_EXPORT_COLUMNS,
    AWS_REGION, 
    S3_BUCKET, 
    s3_key, 
    s3_presign_get, 
    project_index_from_dashboard # <-- add this
)

_s3 = boto3.client("s3", region_name=AWS_REGION)


app = Flask(__name__)
CORS(app)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['PROCESSED_FOLDER'] = 'processed'
app.config['ALLOWED_EXTENSIONS'] = {'pdf'}
app.secret_key = 'your_secret_key_here'

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)

logging.basicConfig(level=logging.DEBUG)

load_dotenv()

model_path = Path(__file__).parent / 'static/en_core_web_sm'
nlp = spacy.load(model_path)

# Local testing
openai.api_key = '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'

# Global terms lists
global equipment_terms, manufacturer_terms, model_terms, universal_terms, competitor_terms
equipment_terms, manufacturer_terms, model_terms, universal_terms, competitor_terms  = [], [], [], [], []

processing_cancelled = threading.Event()

users = {'ays-admin': 'Lx@73z!Q8kV9w#jP', 'login##': '68result96milk'}

# Login page route
@app.route('/', methods=['GET', 'POST'])
def login():
    """
    Login page route for user authentication.
    """
    users = {'ays-admin': 'wordpass!321', 'login##': '68result96milk'}
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        if username in users and users[username] == password:
            session['user'] = username
            return redirect(url_for('upload_file'))
        else:
            flash('Invalid credentials, please try again.')
    return render_template('login.html')

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in app.config['ALLOWED_EXTENSIONS']

@app.route('/analysis', methods=['GET', 'POST'])
def upload_file():
    """
    Handles PDF uploads, processes the file, and renders results.
    """
    if request.method == 'POST':
        # Step 1: Retrieve the uploaded file
        file = request.files.get('file')
        if not file or not allowed_file(file.filename):
            flash("Invalid file type. Please upload a PDF file.")
            return redirect(url_for('upload_file'))

        # Step 2: Secure and save the uploaded file
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)

        try:
            # Step 3: Process the PDF using your processing function
            results = process_pdf_file(filepath)

            # Step 4: Verify and unpack results
            if not results:
                flash("Error: Failed to process the PDF. No results found.")
                return redirect(url_for('upload_file'))

            results_data = results.get('results', {})
            sections = results.get('sections', [])
            acceptance_sections = results.get('acceptance_sections', [])
            total_pages = results.get('total_pages', 0)
            processed_filename = results.get('filename')

            # Debugging Logs
            print("DEBUG: Results Data:", results_data)
            print("DEBUG: Sections:", sections)
            print("DEBUG: Acceptance Sections:", acceptance_sections)

            # Step 5: Validate key components
            if not sections and not acceptance_sections:
                flash("No relevant sections or acceptable manufacturers were found in the document.")
                return redirect(url_for('upload_file'))

            # Step 6: Store results in the session
            session['results_data'] = json.dumps(results)

            # Step 7: Render the results page
            return render_template(
                'template1_7.html',
                results=results_data,
                sections=sections,
                acceptance_sections=acceptance_sections,
                filename=processed_filename,
                total_pages=total_pages
            )

        except Exception as e:
            # Step 8: Handle unexpected errors
            logging.error(f"Exception occurred while processing the file: {e}")
            flash("An unexpected error occurred while processing the file. Please try again.")
            return redirect(url_for('upload_file'))

    # Render upload form if GET request
    return render_template('template1_7.html', results=None)


#@app.route('/view_pdf')
#def view_pdf():
#    """
#    Displays the processed PDF with highlights.
#    """
#    pdf = request.args.get('pdf')
#    if not pdf:
#        return "No PDF specified", 400
#    processed_pdf_path = os.path.join(app.config['PROCESSED_FOLDER'], pdf)
#    if not os.path.exists(processed_pdf_path):
#        return "File not found", 404
#    return send_from_directory(app.config['PROCESSED_FOLDER'], pdf)

#@app.route('/download_tables')
#def download_tables():
#    """
#    Allows users to download results tables as an Excel file.
#    """
 #   results_data = json.loads(session.get('results_data', '{}'))
#    if not results_data:
 #       return "No data available to download.", 400
#    output_filepath = os.path.join(app.config['PROCESSED_FOLDER'], 'results_tables.xlsx')
 #   with pd.ExcelWriter(output_filepath) as writer:
 #       for key, data in results_data.get('results', {}).items():
 #           if data:
 #               pd.DataFrame(data).to_excel(writer, sheet_name=key.capitalize(), index=False)
 #   return send_from_directory(app.config['PROCESSED_FOLDER'], 'results_tables.xlsx', as_attachment=True)

@app.route('/nginx-config')
def nginx_config():
    try:
        with open('/etc/nginx/conf.d/proxy.conf', 'r') as file:
            content = file.read()
        return f"<pre>{content}</pre>", 200
    except Exception as e:
        return str(e), 500

@app.route('/terms', methods=['GET', 'POST'])
def terms():
    filepath = 'terms/UEP_Terms.json'

    # Load existing terms from the JSON file
    try:
        with open(filepath, 'r') as file:
            terms_data = json.load(file)
            logging.debug("Loaded terms data successfully.")
    except FileNotFoundError:
        logging.warning(f"File {filepath} not found. Initializing empty terms data.")
        terms_data = []

    if request.method == 'POST':
        term_type = request.form.get('term_type')  # Get the category (e.g., 'universal')
        new_term = request.form.get(f'new_{term_type}_term', '').strip()  # Get the new term

        logging.debug(f"Received term_type: {term_type}, new_term: {new_term}")

        if term_type and new_term:
            # Find the matching category and add the term
            category_found = False
            for category in terms_data:
                if category['title'].lower() == term_type.replace('_', ' ').lower():
                    category_found = True
                    if new_term not in category['terms']:  # Avoid duplicates
                        category['terms'].append(new_term)
                        logging.debug(f"Added new term '{new_term}' to category '{category['title']}'.")
                    else:
                        logging.debug(f"Term '{new_term}' already exists in category '{category['title']}'.")
                    break

            # If no matching category, log an error and return a message
            if not category_found:
                logging.error(f"Category '{term_type}' not found in terms_data. New term not added.")
                return jsonify({'status': 'error', 'message': f"Category '{term_type}' not found."}), 400

            # Save updated terms to JSON file
            try:
                with open(filepath, 'w') as file:
                    json.dump(terms_data, file, indent=4)
                logging.debug("Successfully saved updated terms to file.")
            except Exception as e:
                logging.error(f"Error saving terms to JSON file: {e}")
                return jsonify({'status': 'error', 'message': str(e)}), 500

        return redirect(url_for('terms') + '#' + term_type)

    # Prepare terms for rendering
    categorized_terms = {
        'equipment_terms': next((x['terms'] for x in terms_data if x['title'].lower() == 'equipment'), []),
        'manufacturer_terms': next((x['terms'] for x in terms_data if x['title'].lower() == 'manufacturer'), []),
        'model_terms': next((x['terms'] for x in terms_data if x['title'].lower() == 'model'), []),
        'universal_terms': next((x['terms'] for x in terms_data if x['title'].lower() == 'universal'), []),
        'competitor_terms': next((x['terms'] for x in terms_data if x['title'].lower() == 'competitor'), []),
    }

    logging.debug(f"Categorized terms before rendering: {categorized_terms}")

    return render_template('terms.html', **categorized_terms)

@app.route('/view_pdf')
def view_pdf():
    pdf = request.args.get('pdf')
    page = request.args.get('page', 1, type=int)
    word = request.args.get('word', '')

    if not pdf:
        return "No PDF specified", 400

    processed_pdf_path = os.path.join('processed', pdf)
    if not os.path.exists(processed_pdf_path):
        logging.error(f"File not found: {processed_pdf_path}")
        return "File not found", 404

    # Pass required information to the template
    return render_template(
        'pdf_viewer1.html',
        pdf_url=url_for('download_file', filename=pdf),
        page_number=page,
        filename=os.path.splitext(pdf)[0],
        word=word
    )


@app.route('/download_section')
def download_section():
    start_page = request.args.get('start_page', type=int)
    stop_page = request.args.get('stop_page', type=int)
    section_name = request.args.get('section_name')
    filename = request.args.get('filename')
    pdf_path = os.path.join('processed', filename)

    if not os.path.exists(pdf_path):
        logging.error(f"File not found: {pdf_path}")
        return "File not found", 404

    try:
        # Create a new PDF with only the desired pages
        reader = PdfReader(pdf_path)
        writer = PdfWriter()

        for page_num in range(start_page - 1, stop_page):
            writer.add_page(reader.pages[page_num])

        output_file = os.path.join('processed', f"{section_name}_Section.pdf")
        with open(output_file, 'wb') as f:
            writer.write(f)

        # Return the file for download
        return send_file(output_file, as_attachment=True)
    except Exception as e:
        logging.error(f"Error generating section PDF: {e}")
        return "Error processing the section.", 500



@app.route('/download_tables')
def download_tables():
    # Retrieve results_data from the session
    results_data = json.loads(session.get('results_data', '{}'))  # Deserialize JSON from session

    if not results_data or not any(results_data.values()):
        return "No data available to download.", 400

    # Create a new Excel file with pandas to hold the tables
    output_file_path = os.path.join(app.config['PROCESSED_FOLDER'], 'results_tables.xlsx')

    # Write data to the Excel file if present
    with pd.ExcelWriter(output_file_path) as writer:
        if results_data['equipment_tables']:
            equipment_df = pd.DataFrame(results_data['equipment_tables'])
            equipment_df.to_excel(writer, sheet_name='Equipment', index=False)
        if results_data['manufacturer_tables']:
            manufacturer_df = pd.DataFrame(results_data['manufacturer_tables'])
            manufacturer_df.to_excel(writer, sheet_name='Manufacturer', index=False)
        if results_data['model_tables']:
            model_df = pd.DataFrame(results_data['model_tables'])
            model_df.to_excel(writer, sheet_name='Model', index=False)
        if results_data['universal_tables']:
            universal_df = pd.DataFrame(results_data['universal_tables'])
            universal_df.to_excel(writer, sheet_name='Universal', index=False)
        if results_data['competitor_tables']:
            competitor_df = pd.DataFrame(results_data['competitor_tables'])
            competitor_df.to_excel(writer, sheet_name='Competitors', index=False)


    # Send the generated file to the user
    return send_file(output_file_path, as_attachment=True)

@app.route('/processed/<path:filename>')
def download_file(filename):
    return send_from_directory(app.config['PROCESSED_FOLDER'], filename)

@app.route('/cancel', methods=['POST'])
def cancel_processing():
    processing_cancelled.set()
    logging.info("Processing has been cancelled by the user.")
    return jsonify({"status": "cancelled"})

# List of valid project types (names only)
VALID_PROJECT_TYPES = [
    "New Development",
    "Rehabilitation (Rehab)",
    "Renovation",
    "Remodeling",
    "Restoration",
    "Expansion (Addition)",
    "Adaptive Reuse",
    "Demolition",
    "Infrastructure Development",
    "Fit-Out (Interior Build-Out)",
    "Green Building (Sustainable Construction)",
    "Tenant Improvement",
    "Historic Preservation",
    "Civil Engineering Works",
    "Seismic Retrofitting",
    "Brownfield Redevelopment",
    "Commercial Development",
    "Residential Development",
    "Industrial Construction",
    "Public Sector Projects"
]

# Flask route for summarizing PDF files
@app.route('/summarize', methods=['GET', 'POST'])
def summarize_pdf():
    summary = None
    info_table = None
    
    if request.method == 'POST':
        file = request.files['file']
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)

            # Extract and summarize the PDF, including extracting information for the table
            summary = extract_and_summarize(filepath)
            
            # Extract information for the table using the combined summary
            info_table = extract_project_information_chunked(summary)
            
            # Debug: Print info_table to check its content
            print("Extracted Project Information:", info_table)

    return render_template('summarize_upload1.html', summary=summary, info_table=info_table)


def generate_summary(text, max_words):
    """Generates a summary of the given text, limited to a specified number of words."""
    MAX_TOKENS_OUTPUT = 750  # Adjust based on word limit needs

    # Create a prompt for generating the summary
    prompt_content = (
        f"Summarize the following text into a coherent and concise final summary of approximately {max_words} words. "
        "Ensure that the summary captures all key aspects and presents them in a clear, conversational style. "
        "Avoid unnecessary details, bullet points, section headers, or lists. Provide a single, cohesive narrative that flows naturally.\n\n" 
        + text
    )

    print(f"Calling OpenAI API to generate a summary of {max_words} words...")

    response = openai.ChatCompletion.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant specialized in summarizing documents."},
            {"role": "user", "content": prompt_content}
        ],
        max_tokens=MAX_TOKENS_OUTPUT,
        temperature=0.5
    )

    # Check if the response is valid
    if 'choices' in response and len(response['choices']) > 0:
        summary_text = response['choices'][0]['message']['content']
        print(f"Summary generated successfully. Length: {len(summary_text.split())} words.")
        return summary_text
    else:
        print("Error: Invalid response from OpenAI API.")
        return "Error: Failed to summarize the document."


def extract_and_summarize(pdf_path):
    """Extracts and summarizes text from a PDF document following a step-by-step iterative process."""
    doc = fitz.open(pdf_path)
    total_pages = len(doc)

    # Restrict pages to review based on total page count
    if total_pages <= 100:
        pages_to_review = 20
    else:
        pages_to_review = 50

    combined_summary = ""  # This will store the progressively built summary

    # Process pages in chunks of pages_to_review
    print(f"Total pages to process: {total_pages}")
    for start_page in range(0, pages_to_review, 10):  # Process in chunks of 10 pages
        end_page = min(start_page + 10, pages_to_review)
        extracted_text = extract_pages(doc, start_page, end_page)

        if start_page == 0:
            # If this is the first chunk, summarize to 250 words
            print(f"Summarizing first chunk from pages {start_page} to {end_page}")
            combined_summary = generate_summary(extracted_text, max_words=250)
        else:
            # Combine the previous summary with the new extracted text
            combined_text = combined_summary + "\n\n" + extracted_text
            # Generate a new summary of 500 words or less
            print(f"Combining and summarizing chunk from pages {start_page} to {end_page}")
            combined_summary = generate_summary(combined_text, max_words=500)

    doc.close()

    # Final summary is now stored in combined_summary
    print("Final summary generated.")
    return combined_summary or "Summary not available"


def extract_pages(doc, start_page, end_page):
    """Extracts text from a range of pages in a PDF document."""
    extracted_text = ""
    for page_num in range(start_page, end_page):
        if page_num >= 0 and page_num < len(doc):  # Check bounds
            page = doc.load_page(page_num)
            extracted_text += page.get_text()
    return extracted_text


def extract_project_information_chunked(text):
    """
    Extracts the project information by splitting the text into chunks and sending it to GPT.
    """
    MAX_TOKENS_INPUT = 1200  # Set a safe limit for tokens to prevent exceeding context length
    MAX_TOKENS_OUTPUT = 250  # Limit output tokens to keep it concise
    chunk_size = 1000  # Smaller chunk size to ensure safety within token limits
    project_info_chunks = []

    # Split text into manageable chunks
    chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]

    # Process chunks concurrently
    with ThreadPoolExecutor(max_workers=5) as executor:  # Use directly without `concurrent.`
        futures = [executor.submit(process_chunk, chunk, MAX_TOKENS_OUTPUT) for chunk in chunks]
        for future in as_completed(futures):  # Use directly without `concurrent.`
            result = future.result()
            if "Error" not in result:  # Only add valid results
                project_info_chunks.append(result)
            else:
                print(f"Error in processing chunk: {result}")  # Debugging information

    # Combine project info results
    combined_project_info = "\n".join(project_info_chunks)
    extracted_info = extract_info_from_response(combined_project_info)

    # Debug: Print the extracted_info to verify the correct format
    print("Extracted Info from Response:", extracted_info)

    return extracted_info

def process_chunk(chunk, max_tokens_output):
    """Processes a single chunk by sending it to the OpenAI API and returning the response."""
    try:
        # Use GPT-4o Mini to infer project details from each chunk
        response = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": (
                    "You are an assistant specialized in analyzing construction documents. Your task is to identify "
                    "specific roles and their associated entities from the text. Look for and extract the following information:\n"
                    "- **Owner**: The entity or organization that owns the project.\n"
                    "- **Owner's Representative**: The person or organization representing the owner in the project.\n"
                    "- **Engineering Firm**: The engineering firm involved in planning or specifications.\n"
                    "- **Architect**: The architect or architectural firm associated with the project.\n"
                    "- **Project Type**: The type of project (e.g., New Development, Renovation, Demolition).\n\n"
                    "Provide the extracted information clearly in this format:\n"
                    "- Owner: [Owner Name]\n"
                    "- Owner's Representative: [Representative Name]\n"
                    "- Engineering Firm: [Engineering Firm Name]\n"
                    "- Architect: [Architect Name]\n"
                    "- Project Type: [Valid Project Type]\n\n"
                    "If not mentioned, respond with 'Not Specified'. Analyze the following text carefully:\n" + chunk
                )},
                {"role": "user", "content": f"The following is a text chunk extracted from a construction document:\n{chunk}"}
            ],
            max_tokens=max_tokens_output,
            temperature=0.5,
            timeout=120  # Set a higher timeout value
        )

        # Check if response is valid and return the result
        if 'choices' in response and len(response['choices']) > 0:
            return response.choices[0].message['content']
        else:
            print("Error: Invalid response from OpenAI API.")
            return "Error: Failed to extract project information."

    except Exception as e:
        print(f"Exception occurred while processing chunk: {e}")
        return "Error: An exception occurred during the extraction process."

def extract_info_from_response(response_text):
    """
    Extracts the owner, owner's representative, engineering firm, architect, and project type
    from the GPT-4o Mini response. Ensures Project Type is only selected from the predefined list.
    """
    extracted_info = {
        "Owner": "Not Specified",
        "Owner's Representative": "Not Specified",
        "Engineering Firm": "Not Specified",
        "Architect": "Not Specified",
        "Project Type": "Not Specified"
    }

    # Enhanced parsing logic to match lines and extract the relevant information
    for line in response_text.split('\n'):
        if 'Owner:' in line and 'Not Specified' not in line:
            extracted_info["Owner"] = line.split(':', 1)[1].strip()
        elif "Owner's Representative:" in line and 'Not Specified' not in line:
            extracted_info["Owner's Representative"] = line.split(':', 1)[1].strip()
        elif 'Engineering Firm:' in line and 'Not Specified' not in line:
            extracted_info["Engineering Firm"] = line.split(':', 1)[1].strip()
        elif 'Architect:' in line and 'Not Specified' not in line:
            extracted_info["Architect"] = line.split(':', 1)[1].strip()
        elif 'Project Type:' in line and 'Unknown' not in line:
            project_type = line.split(':', 1)[1].strip()
            # Validate Project Type against the list of valid types
            if project_type in VALID_PROJECT_TYPES:
                extracted_info["Project Type"] = project_type
            else:
                extracted_info["Project Type"] = "Unknown"  # Set to 'Unknown' if not in list

    # Debug: Check if all required keys have valid data
    print("Extracted Information Details:", extracted_info)

    return extracted_info



import os
import io
import base64
import smtplib
import logging
import pandas as pd
from flask import Flask, request, jsonify, after_this_request
from werkzeug.utils import secure_filename
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from email.mime.text import MIMEText

# SMTP Config for Office365
SMTP_SERVER = "smtp.office365.com"
SMTP_PORT = 587
SMTP_USERNAME = "uep@areyouspecified.com"
SMTP_PASSWORD = "HH#d3t@12345%%"
SENDER_EMAIL = "uep@areyouspecified.com"

# ---------------------------------------
# HELPER: Send email with attachments
# ---------------------------------------
from email.mime.image import MIMEImage


@app.get("/s3/ping")
def s3_ping():
    key = s3_key(S3_UPLOAD_PREFIX, "app-ping.txt")
    s3_upload_bytes(b"hello from flask", key)
    url = s3_presign_get(key, expires=600)
    return jsonify({"key": key, "presigned_url": url})

def send_email_with_attachments(to_address, subject, body_text, attachments, logo_path=None, original_message_id=None):
    msg = MIMEMultipart('related')
    msg['From'] = SENDER_EMAIL
    msg['To'] = to_address
    msg['Subject'] = subject

    # Add reply headers if available
    if original_message_id:
        msg['In-Reply-To'] = original_message_id
        msg['References'] = original_message_id

    # Create alternative part for HTML body
    alt_part = MIMEMultipart('alternative')
    msg.attach(alt_part)

    # Attach the HTML body
    alt_part.attach(MIMEText(body_text, 'html'))

    # Attach inline logo image
    if logo_path and os.path.isfile(logo_path):
        with open(logo_path, 'rb') as img_file:
            img = MIMEImage(img_file.read(), name=os.path.basename(logo_path))
            img.add_header('Content-ID', '<logo_cid>')
            img.add_header('Content-Disposition', 'inline', filename='logo.png')
            msg.attach(img)

    # Attach other files
    for path in attachments:
        with open(path, 'rb') as f:
            part = MIMEApplication(f.read())
            part.add_header('Content-Disposition', 'attachment', filename=os.path.basename(path))
            msg.attach(part)

    # Send the message
    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(SENDER_EMAIL, to_address, msg.as_string())


# ---------------------------------------
# HELPER: Write Excel with all 7 tabs
# ---------------------------------------
import pandas as pd
import logging

def write_results_to_excel(results, excel_path):
    try:
        with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
            workbook = writer.book
            header_format = workbook.add_format({
                'bold': True,
                'bg_color': '#F58220',
                'font_color': '#FFFFFF',
                'border': 1
            })
            content_format = workbook.add_format({
                'border': 1
            })

            def write_df_to_sheet(df, sheet_name):
                df = df[[col for col in df.columns if 'link' not in col.lower()]]

                if df.empty:
                    df = pd.DataFrame(columns=[''])

                df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1, header=False)
                worksheet = writer.sheets[sheet_name]

                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                    worksheet.set_column(col_num, col_num, max(15, len(str(value)) + 2))

                for row_num in range(1, len(df) + 1):
                    for col_num in range(len(df.columns)):
                        worksheet.write(row_num, col_num, df.iloc[row_num - 1, col_num], content_format)

                worksheet.freeze_panes(1, 0)

            # Sections
            df_sections = pd.DataFrame(results.get('sections', []))
            write_df_to_sheet(df_sections, 'Sections')

            # Acceptance Sections (renamed to 'Specified Manufacturers')
            df_accept = pd.DataFrame(results.get('acceptance_sections', []))
            write_df_to_sheet(df_accept, 'Specified Manufacturers')

            # Keyword Sheets with updated sheet names
            category_mapping = {
                'manufacturer': 'Your Manufacturers',
                'competitor': 'Comp Manufacturers',
                'equipment': 'Equipment',
                'model': 'Model',
                'universal': 'BOD by Section',
            }

            results_data = results.get('results', {})
            for key, sheet_name in category_mapping.items():
                records = results_data.get(key, [])
                df = pd.DataFrame(records)
                if df.empty:
                    df = pd.DataFrame(columns=['Word', 'Page', 'Section', 'Section Name'])
                else:
                    df = df[['Word', 'Page', 'Section', 'Section Name']]
                write_df_to_sheet(df, sheet_name)

        logging.info(f"✅ Excel workbook written to {excel_path}")

    except Exception as e:
        logging.error(f"Error writing Excel workbook: {e}", exc_info=True)

# ---------------------------------------
# HELPER: Build nicely formatted email body
# ---------------------------------------
def generate_email_body(original_subject, total_keywords, manufacturer_rows, competitor_rows, recommendation, logo_base64, ays_id):
    html = f"""
    <html>
    <body>
    <img src="cid:logo_cid" alt="AYS Logo" style="width:200px; height:auto; margin-bottom:20px;"><br>

    <p>Hello,</p>
    <p>Thank you for using <strong>Are You Specified (AYS)</strong>!</p>
    <p>Your analysis is complete for:<br>
    <strong>Original Email Subject:</strong> {original_subject}</p>
    <p><strong>AYS ID:</strong> {ays_id}</p>

    <p><strong>Summary:</strong><br>
    - Total Keywords: {total_keywords}<br>
    - Manufacturers: {len(manufacturer_rows)}<br>
    - Competitors: {len(competitor_rows)}<br>
    - Recommendation: <strong>{recommendation}</strong></p>

    <p><strong>Manufacturer Terms:</strong></p>
    {format_html_table(manufacturer_rows)}

    <p><strong>Competitor Terms:</strong></p>
    {format_html_table(competitor_rows)}

    <p>Attached you will find:<br>
    - The highlighted PDF you submitted.<br>
    - A complete Excel workbook with analysis tables.
    - Find all of the your analytics for your submissions here: https://beta.areyouspecified.com/dashboard</p>

    <p>Best regards,<br>The AYS Team</p>
    </body>
    </html>
    """
    return html

# ✅ Load and encode the logo
logo_path = os.path.join(app.root_path, 'static', 'logo.png')
with open(logo_path, 'rb') as image_file:
    logo_base64 = base64.b64encode(image_file.read()).decode('utf-8')


def format_html_table(rows):
    if not rows:
        return "<p>None found.</p>"

    headers = ["Word", "Page", "Section", "Section Name"]

    table = '<table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">'
    table += "<tr style='background-color:#F58220;color:white;font-weight:bold;'>"
    for header in headers:
        table += f"<th>{header}</th>"
    table += "</tr>"

    for row in rows:
        table += "<tr>"
        for h in headers:
            table += f"<td>{row.get(h, '')}</td>"
        table += "</tr>"

    table += "</table>"
    return table

import fitz  # PyMuPDF

def get_highlighted_pages(pdf_path):
    doc = fitz.open(pdf_path)
    highlighted_pages = set()
    for page_number in range(len(doc)):
        page = doc[page_number]
        for annot in page.annots():
            if annot.type[0] == 8:  # 8 = Highlight
                highlighted_pages.add(page_number)
                break
    return sorted(highlighted_pages)


import logging
from collections import defaultdict
import fitz  # PyMuPDF

def create_highlighted_only_pdf(highlighted_pdf_path, results, output_path):
    src_doc = fitz.open(highlighted_pdf_path)
    new_doc = fitz.open()
    added_pages = 0

    for i in range(len(src_doc)):  # Check every page
        page = src_doc[i]
        has_highlight = False

        for annot in page.annots():
            if annot.type[0] == 8:  # Highlight
                has_highlight = True
                break

        if has_highlight:
            new_doc.insert_pdf(src_doc, from_page=i, to_page=i)
            logging.debug(f"✅ Included page {i} with highlight(s).")
            added_pages += 1
        else:
            logging.debug(f"⛔ Skipped page {i} (no highlights).")

    if added_pages > 0:
        new_doc.save(output_path)
        logging.info(f"✅ Highlighted-only PDF saved: {output_path} with {added_pages} pages")
    else:
        logging.warning("⚠️ No valid highlighted pages found — no output generated.")

    src_doc.close()
    new_doc.close()

# ---------------------------------------
# Format Email Tables
# ---------------------------------------
def format_table(rows):
    if not rows:
        return "None found."

    col_widths = [15, 6, 8, 25]  # Adjust as needed
    headers = ["Word", "Page", "Section", "Section Name"]
    output = []

    # Header row
    header_line = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
    separator = "-+-".join("-" * col_widths[i] for i in range(len(headers)))
    output.append(header_line)
    output.append(separator)

    # Data rows
    for row in rows:
        line = " | ".join(str(row.get(h, "")).ljust(col_widths[i]) for i, h in enumerate(headers))
        output.append(line)

    return "\n".join(output)


from xhtml2pdf import pisa

def html_to_pdf(html_content, output_path):
    with open(output_path, "wb") as f:
        pisa.CreatePDF(io.StringIO(html_content), dest=f)


# ---------------------------------------
# FLASK ROUTE: /api/process
# ---------------------------------------
import shutil

@app.route('/api/process', methods=['POST'])
def api_process_pdf():
    try:
        data = request.get_json()

        # ✅ Validate
        filename = data.get('AttachmentName')
        content_b64 = data.get('AttachmentContent')
        original_receiver = data.get('From')
        original_subject = data.get('Subject')
        original_message_id = data.get('MessageID')

        if not all([filename, content_b64, original_receiver, original_subject]):
            return jsonify({"error": "Missing required fields."}), 400

        # ✅ Create AYS ID
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        ays_id = f"AYS-{timestamp}"

        # ✅ Decode and save original PDF
        secure_name = secure_filename(filename)
        upload_path = os.path.join(app.config['UPLOAD_FOLDER'], secure_name)
        with open(upload_path, 'wb') as f:
            f.write(base64.b64decode(content_b64))

        # ✅ Process PDF
        results = process_pdf_file(upload_path)
        if not results or not results.get('results'):
            return jsonify({"error": "Processing failed."}), 500

        results_data = results['results']

        # ✅ Count keywords
        total_keywords = sum(len(t) for t in results_data.values() if t)

        # ✅ Extract manufacturer and competitor terms
        manufacturer_rows = [
            {
                "Word": row.get('Word', ''),
                "Page": row.get('Page', ''),
                "Section": row.get('Section', ''),
                "Section Name": row.get('Section Name', '')
            }
            for row in results_data.get('manufacturer', [])
        ]

        competitor_rows = [
            {
                "Word": row.get('Word', ''),
                "Page": row.get('Page', ''),
                "Section": row.get('Section', ''),
                "Section Name": row.get('Section Name', '')
            }
            for row in results_data.get('competitor', [])
        ]

        # ✅ Write Excel with ALL 7 tabs
        excel_filename = f"tables_{secure_name.rsplit('.', 1)[0]}.xlsx"
        excel_path = os.path.join(app.config['PROCESSED_FOLDER'], excel_filename)
        write_results_to_excel(results, excel_path)

        # ✅ Get Highlighted PDF
        highlighted_pdf_filename = results.get('filename')
        if not highlighted_pdf_filename:
            return jsonify({"error": "Highlighted PDF missing."}), 500

        highlighted_pdf_path = os.path.join(app.config['PROCESSED_FOLDER'], highlighted_pdf_filename)
        if not os.path.isfile(highlighted_pdf_path):
            return jsonify({"error": "Highlighted PDF not found."}), 500

        # ✅ Create highlighted-only PDF
        original_base = os.path.splitext(filename)[0]
        highlighted_only_pdf_path = os.path.join(
            app.config['PROCESSED_FOLDER'],
            f"only_highlights_{original_base}.pdf"
        )
        create_highlighted_only_pdf(highlighted_pdf_path, results, highlighted_only_pdf_path)

        # ✅ Determine recommendation
        has_mfg = bool(manufacturer_rows)
        has_comp = bool(competitor_rows)

        if has_mfg and has_comp:
            recommendation = "You and your competitor are specified. Bid this opportunity!"
            subject_summary = "Specified - Bid!"
        elif has_mfg:
            recommendation = "You are Specified! Bid this opportunity!"
            subject_summary = "Specified - Bid!"
        elif has_comp:
            recommendation = "Your competitor is specified - Review this opportunity."
            subject_summary = "Competitor Specified"
        else:
            recommendation = "You are not specified. Pass on this opportunity"
            subject_summary = "Not Specified - Do not Bid!"

        # ✅ Build and convert email body to PDF
        email_body = generate_email_body(
            original_subject,
            total_keywords,
            manufacturer_rows,
            competitor_rows,
            recommendation,
            logo_base64,
            ays_id
        )
        email_pdf_path = os.path.join(app.config['PROCESSED_FOLDER'], f"{ays_id}_email_summary.pdf")
        html_to_pdf(email_body, email_pdf_path)

        # ✅ Log to dashboard
        log_results_to_excel(
            ays_id=ays_id,
            from_email=original_receiver,
            project_name=original_subject,
            manufacturer_terms=[row["Word"] for row in manufacturer_rows],
            recommendation=subject_summary
        )

        # ✅ Rename files based on project name (email subject) before zipping
        project_name_clean = secure_filename(original_subject)

        renamed_files = []
        rename_map = {
            highlighted_pdf_path: f"{project_name_clean}_highlighted.pdf",
            highlighted_only_pdf_path: f"{project_name_clean}_only_highlights.pdf",
            excel_path: f"{project_name_clean}_tables.xlsx",
            email_pdf_path: f"{project_name_clean}_email_summary.pdf"
        }

        for old_path, new_filename in rename_map.items():
            new_path = os.path.join(app.config['PROCESSED_FOLDER'], new_filename)
            os.rename(old_path, new_path)
            renamed_files.append(new_path)

        # ✅ Bundle renamed files into a ZIP
        import zipfile
        import tempfile

        with tempfile.NamedTemporaryFile(delete=False, suffix=".zip") as tmp_zip:
            with zipfile.ZipFile(tmp_zip.name, 'w') as zipf:
                for file_path in renamed_files:
                    zipf.write(file_path, arcname=os.path.basename(file_path))

            zip_download_path = tmp_zip.name
        
        zip_name_for_download = f"{project_name_clean}_AYS_Report.zip"
        return send_file(
            zip_download_path,
            as_attachment=True,
            download_name=zip_name_for_download
        )


    except Exception as e:
        logging.error(f"API Exception: {e}", exc_info=True)
        return jsonify({"error": "Internal Server Error"}), 500


def log_results_to_excel(from_email, manufacturer_terms, recommendation, ays_id, project_name):
    dashboard_path = os.path.join(app.root_path, 'data', 'ays_dashboard.xlsx')
    os.makedirs(os.path.dirname(dashboard_path), exist_ok=True)

    now = datetime.now()
    timestamp_str = now.strftime('%m/%d/%Y %H:%M')

    new_row = pd.DataFrame([{
        'AYS ID': ays_id,
        'Date': timestamp_str,
        'Email': from_email,
        'Project Name': project_name,
        'Manufacturer Terms': ", ".join(manufacturer_terms) if manufacturer_terms else "None",
        'Recommendation': recommendation
    }])


    logging.debug("📄 New row to add to Excel:")
    logging.debug(new_row.to_dict(orient="records")[0])

    try:
        if os.path.exists(dashboard_path):
            old_df = pd.read_excel(dashboard_path, dtype=str)
            full_df = pd.concat([old_df, new_row], ignore_index=True)
        else:
            full_df = new_row

        # Convert 'Date' column to datetime for safe sorting
        full_df['Date'] = pd.to_datetime(full_df['Date'], errors='coerce', format='%m/%d/%Y %H:%M')
        full_df = full_df.sort_values(by='Date', ascending=False)

        # Convert Date back to formatted string for Excel export
        full_df['Date'] = full_df['Date'].dt.strftime('%m/%d/%Y %H:%M')

        full_df.to_excel(dashboard_path, index=False)
        logging.info(f"✅ Dashboard updated at {dashboard_path}")

    except Exception as e:
        logging.error(f"Failed to log to dashboard: {e}", exc_info=True)

@app.route('/dashboard')
def view_dashboard():
    dashboard_path = DASHBOARD_XLSX
    if not os.path.isfile(dashboard_path):
        return "<p>No dashboard data yet.</p>"

    df = pd.read_excel(dashboard_path, dtype=str).fillna("")
    needed = [
        "Date", "AYS ID", "Email", "Project Name",
        "Manufacturer Terms", "Recommendation", "Download URL"
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = ""

    # Sort newest first by Date
    try:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.sort_values(by="Date", ascending=False)
        df["Date"] = df["Date"].dt.strftime("%m/%d/%Y")
    except Exception:
        pass

    table_columns = needed
    table_data = df[table_columns].to_dict(orient="records")

    counts = df["Recommendation"].value_counts()
    labels = counts.index.tolist()
    values = counts.values.tolist()

    return render_template(
        "dashboard1.html",
        table_columns=table_columns,
        table_data=table_data,
        chart_labels=labels,
        chart_values=values,
    )


# === UPDATED: /dashboard/download (uses the shared path) ===
from helpers_async_s3_0_3 import DASHBOARD_XLSX, CUSTOMER_EXPORT_COLUMNS
import tempfile
from datetime import datetime
from flask import after_this_request

@app.route('/dashboard/download')
def download_dashboard_excel():
    if not os.path.isfile(DASHBOARD_XLSX):
        return "No dashboard data yet.", 404

    df = pd.read_excel(DASHBOARD_XLSX, dtype=str).fillna("")

    # Keep only customer-safe columns (ignore any missing gracefully)
    cols = [c for c in CUSTOMER_EXPORT_COLUMNS if c in df.columns]
    if not cols:
        return "No customer-visible data to export.", 404

    df = df[cols]

    # Keep only rows that have basic customer info
    if "Email" in df.columns:
        df = df[df["Email"].str.strip() != ""]
    if "Project Name" in df.columns:
        df = df[df["Project Name"].str.strip() != ""]

    if df.empty:
        return "No customer rows available to export.", 404

    # Write to a temp file and send it; then delete it
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    tmp_path = tmp.name
    tmp.close()
    df.to_excel(tmp_path, index=False)

    @after_this_request
    def _cleanup(response):
        try:
            os.unlink(tmp_path)
        except Exception:
            pass
        return response

    filename = f"ays_dashboard_{datetime.now().strftime('%Y%m%d')}.xlsx"
    return send_file(tmp_path, as_attachment=True, download_name=filename)



@app.route('/project')
def start_project():
    return render_template('project_submitted1.html')

import base64
import os
import base64
import requests
from flask import request, render_template, redirect, url_for

from datetime import datetime

@app.post('/project-process')
def project_process():
    subject = request.form.get('Subject') or "Untitled_Project"
    email = request.form.get('From') or "unknown@example.com"
    files = request.files.getlist('files')

    if not files:
        return jsonify({"error": "No files uploaded"}), 400

    project_id = make_project_id(subject)
    job_ids = []

    callbacks = dict(
        process_pdf_file=process_pdf_file,
        create_highlighted_only_pdf=create_highlighted_only_pdf,
        generate_email_body=generate_email_body,
        logo_base64=logo_base64,
        write_results_to_excel=write_results_to_excel,  # <- add this back
    )


    submitted_at_iso = datetime.utcnow().isoformat(timespec='seconds') + "Z"  # e.g. 2025-08-20T02:11:32Z

    for file in files:
        if not file or not file.filename:
            continue

        payload = {
            "AttachmentName": file.filename,
            "AttachmentContent": base64.b64encode(file.read()).decode('utf-8'),
            "From": email,
            "Subject": subject,
            "MessageID": None,
            "ProjectID": project_id,
            "SubmittedAt": submitted_at_iso,  # <-- add this
        }

        def job_fn(job_id, payload_inner):
            try:
                set_job(job_id, state="STARTED", project_id=payload_inner["ProjectID"])
                res = run_pipeline_to_s3(
                    job_id=job_id,
                    payload=payload_inner,
                    callbacks=callbacks,
                    upload_folder=app.config['UPLOAD_FOLDER'],
                    processed_folder=app.config['PROCESSED_FOLDER'],
                )
                set_job(job_id, state="SUCCESS", info=res, project_id=payload_inner["ProjectID"])

                # Append one dashboard row per completed doc
                log_completed_job_row(
                    ays_id=res["ays_id"],
                    from_email=payload_inner.get("From") or "",
                    project_name=payload_inner.get("Subject") or "",
                    manufacturer_terms=res.get("manufacturer_terms"),
                    recommendation=res.get("recommendation"),
                    project_id=res["project_id"],
                    doc_folder=res["doc_folder"],
                    zip_key=res["zip_key"],
                    job_id=job_id,
                    submitted_at=payload_inner.get("SubmittedAt"),   # <-- pass it through
                )

            except Exception as e:
                logging.exception("Background job failed")
                set_job(job_id, state="FAILURE", info={"error": str(e)}, project_id=payload_inner["ProjectID"])

        job_id = submit_job(job_fn, payload)
        job_ids.append(job_id)

    set_job(project_id, state="PROJECT", jobs=job_ids, subject=subject)
    wants_json = (
        request.accept_mimetypes.best == 'application/json'
        or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
    )
    return (jsonify({'project_id': project_id, 'job_ids': job_ids})
            if wants_json else render_template('project_submitted1.html', project_id=project_id, job_ids=job_ids))


@app.get('/status/<job_id>')
def job_status(job_id):
    rec = get_job(job_id)
    if not rec:
        return jsonify({"error": "unknown job"}), 404
    state = rec.get("state", "QUEUED")
    info = rec.get("info", {})
    return jsonify({"job_id": job_id, "state": state, "info": info})


@app.get("/project-status/<project_id>")
def project_status(project_id):
    rec = get_job(project_id)
    if not rec:
        return jsonify({"error":"unknown project"}), 404
    job_ids = rec.get("jobs", [])
    statuses = []
    for jid in job_ids:
        j = get_job(jid) or {}
        statuses.append({
            "job_id": jid,
            "state": j.get("state"),
            "doc_folder": (j.get("info") or {}).get("doc_folder"),
            # "zip_url": (j.get("info") or {}).get("zip_url"),  # optional
        })
    return jsonify({
        "project_id": project_id,
        "subject": rec.get("subject"),
        "jobs": statuses,
        "index_url": rec.get("index_url")  # <- expose S3 console link when ready
    })


@app.get("/result/<job_id>")
def job_result(job_id):
    rec = get_job(job_id)
    if not rec:
        return jsonify({"error":"unknown job"}), 404
    if rec.get("state") != "SUCCESS":
        return jsonify({"error":"not ready"}), 409
    info = rec.get("info", {})
    zip_key = info.get("zip_key")
    if not zip_key:
        return jsonify({"error":"missing artifact"}), 500
    url = s3_presign_get(zip_key, expires=3600)
    return redirect(url, code=302)

from helpers_async_s3_0_3 import get_job, s3_presign_get, DASHBOARD_XLSX
import pandas as pd
import logging
from flask import redirect, jsonify

@app.get("/dl/<job_id>")
def dl_always(job_id):
    # 1) Try in-memory job store
    rec = get_job(job_id)
    zip_key = None
    if rec and rec.get("state") == "SUCCESS":
        zip_key = (rec.get("info") or {}).get("zip_key")

    # 2) Fallback to dashboard workbook (survives restarts)
    if not zip_key:
        try:
            df = pd.read_excel(DASHBOARD_XLSX, dtype=str).fillna("")
            row = df.loc[df.get("Job ID", "") == job_id]
            if not row.empty:
                zip_key = row.iloc[0].get("S3 Zip Key", "")
        except Exception as e:
            logging.error(f"dl_always: failed reading dashboard for {job_id}: {e}")

    if not zip_key:
        return jsonify({"error": "unknown or incomplete job"}), 404

    # 3) Presign fresh every click
    url = s3_presign_get(zip_key, expires=3600)  # 1-hour URL, refreshed on each click
    return redirect(url, code=302)

from helpers_async_s3_0_3 import (
    s3_list_dir, s3_presign_get, is_allowed_key, _clean_prefix, BROWSER_ROOT
)

@app.get("/explorer")
def explorer_page():
    # Simple HTML shell; the JS will call /api/files
    return render_template("explorer1.html")

@app.get("/api/files")
def api_files():
    # ?prefix=results/Project_X/...  (or blank for root)
    raw = request.args.get("prefix", BROWSER_ROOT)
    data = s3_list_dir(raw)
    return jsonify(data)

@app.get("/dl/<path:key>")
def download_by_key(key):
    # Stable app URL that 302s to a short-lived S3 presigned URL.
    if not is_allowed_key(key):
        return jsonify({"error": "forbidden"}), 403
    url = s3_presign_get(key, expires=3600)
    return redirect(url, code=302)

@app.get("/api/explorer")
def api_explorer():
    path = (request.args.get("path") or "").strip()
    if path in ("", "/"):
        path = "results/"
    if not path.endswith("/"):
        path += "/"

    if path.lower() == "results/":
        idx = project_index_from_dashboard()
        items = []
        for pid, meta in idx.items():
            items.append({
                "type": "project",
                "name": meta.get("project_name") or pid,
                "date": meta.get("date", ""),
                "email": meta.get("email", ""),
                "key": f"results/{pid}/",
                "sort_key": meta.get("sort_key", 0),
            })
        # ✅ newest first by numeric sort_key
        items.sort(key=lambda x: x.get("sort_key", 0), reverse=True)
        return jsonify({"path": "results/", "items": items})

    # Inside a project: list real S3 prefixes and files
    prefix = path
    try:
        resp = _s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix, Delimiter="/")
    except Exception as e:
        logging.exception("list_objects_v2 failed")
        return jsonify({"path": path, "items": [], "error": "list failed"}), 500

    # Directories under this prefix
    dirs = []
    for cp in resp.get("CommonPrefixes", []):
        p = cp.get("Prefix", "")
        if not p:
            continue
        # last folder name
        name = p.rstrip("/").split("/")[-1]
        dirs.append({"type": "dir", "name": name, "key": p})

    # Files under this prefix
    files = []
    for obj in resp.get("Contents", []):
        key = obj.get("Key", "")
        if not key or key.endswith("/") or key == prefix:
            continue
        files.append({
            "type": "file",
            "name": key.rsplit("/", 1)[-1],
            "key": key,
            "size": obj.get("Size", 0),
            "modified": obj.get("LastModified").isoformat() if obj.get("LastModified") else "",
        })

    return jsonify({"path": path, "items": dirs + files})

@app.get("/dl/by-key")
def dl_by_key():
    """
    302 to a fresh presigned URL for the exact S3 key passed in.
    Use this for file downloads from the explorer.
    """
    key = (request.args.get("key") or "").lstrip("/")
    # basic guardrails (keep simple)
    if not key or ".." in key:
        return jsonify({"error": "bad key"}), 400
    try:
        url = s3_presign_get(key, expires=3600)
        return redirect(url, code=302)
    except Exception:
        logging.exception("presign failed")
        return jsonify({"error": "presign failed"}), 500



if __name__ == "__main__":
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    os.makedirs(app.config['PROCESSED_FOLDER'], exist_ok=True)
    app.run(host='0.0.0.0', port=8000)


