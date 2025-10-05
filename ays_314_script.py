import os
import re
import logging
import json
import spacy
import pandas as pd
from PyPDF2 import PdfReader
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from pathlib import Path
import fitz  # PyMuPDF
from fpdf import FPDF
from pdf2image import convert_from_path
from pytesseract import pytesseract
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from PyPDF2 import PdfReader


# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Load environment variables
load_dotenv()

# Set paths for file handling
UPLOAD_FOLDER = 'uploads'
PROCESSED_FOLDER = 'processed'
ALLOWED_EXTENSIONS = {'pdf'}

# Ensure necessary folders exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(PROCESSED_FOLDER, exist_ok=True)

# Load the spaCy model
model_path = Path(__file__).parent / 'static/en_core_web_sm'
nlp = spacy.load(model_path)

# Initialize global variables
search_terms = {}
highlight_colors = {}
last_valid_section_name = 'Unknown'
last_valid_section_id = 'Unknown'

def load_search_terms(filepath="terms/UEP_Terms.json"):
    global search_terms, highlight_colors
    try:
        with open(filepath, 'r') as f:
            json_data = json.load(f)
        search_terms = {item['title']: item['terms'] for item in json_data}
        highlight_colors = {
            item['title']: tuple(int(item['color'][i:i + 2], 16) / 255 for i in (1, 3, 5))
            for item in json_data
        }
        logging.info(f"Loaded search terms: {json.dumps(search_terms, indent=2)}")
    except (FileNotFoundError, json.JSONDecodeError) as e:
        logging.error(f"Error loading search terms from JSON file: {e}")

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def is_image_based_pdf(pdf_path):
    """
    Check if the PDF contains selectable text or is image-based.
    """
    try:
        doc = fitz.open(pdf_path)
        for page_num in range(len(doc)):
            text = doc[page_num].get_text()
            if text.strip():  # If text is found, it's not image-based
                logging.info(f"Page {page_num + 1}: Standard text detected.")
                return False
        logging.info("No standard text found. PDF is image-based.")
        return True  # No text found; assume image-based
    except Exception as e:
        logging.error(f"Error checking if PDF is image-based: {e}")
        raise


def extract_text_from_pdf(pdf_path):
    """
    Extracts text from each page of a PDF while preserving whitespace and returns it as a list by page.
    """
    text_by_page = []
    try:
        with open(pdf_path, 'rb') as file:
            reader = PdfReader(file)
            for page in reader.pages:
                extracted_text = page.extract_text()
                text_by_page.append(extracted_text if extracted_text else '')  # Preserve raw formatting
    except Exception as e:
        logging.error(f"Error extracting text from PDF: {e}")
    return text_by_page


def perform_ocr(pdf_path):
    """
    Perform OCR on an image-based PDF, save text to a file, and return the path to the text file and the total number of pages.
    """
    output_text_file = f"{Path(pdf_path).stem}_ocr_output.txt"
    logging.info("Starting OCR conversion...")
    num_pages = 0  # Counter for pages processed

    try:
        # Open the output text file for writing
        with open(output_text_file, 'w', encoding='utf-8') as text_file:
            for page_num, image in enumerate(convert_from_path(pdf_path, dpi=300), start=1):
                num_pages += 1
                ocr_text = pytesseract.image_to_string(image, lang='eng')
                text_file.write(f"=== Page {page_num} ===\n{ocr_text}\n\n\f")
                logging.info(f"OCR completed for page {page_num}.")
    except Exception as e:
        logging.error(f"Error during OCR conversion: {e}")
        raise e

    return output_text_file, num_pages  # Return a tuple with the text file path and the page count

def extract_footer(page, footer_height_ratio=0.1):
    """
    Extract the footer from a page using its dimensions.
    The footer is defined as the bottom 'footer_height_ratio' of the page height.
    """
    try:
        # Get the page's bounding box
        text_blocks = page.get_text("blocks")
        page_height = page.rect.height
        footer_height = footer_height_ratio * page_height

        # Filter blocks that are within the footer region
        footer_text = []
        for block in text_blocks:
            block_y1 = block[1]  # Top of the block
            block_y2 = block[3]  # Bottom of the block
            if block_y2 >= (page_height - footer_height):  # If block overlaps the footer region
                footer_text.append(block[4])  # Extract block text

        return "\n".join(footer_text)
    except Exception as e:
        logging.error(f"Error extracting footer: {e}")
        return ""

def identify_sections(pdf_page, footer_height_ratio=0.1):
    """
    Identify section patterns from the footer of the page using its dimensions.
    Match multiple formats such as 'XX XX XX', '210100', '210100-1', etc.
    """
    sections = {}

    # Extract the footer text using page dimensions
    footer_text = extract_footer(pdf_page, footer_height_ratio)
    logging.debug(f"Extracted footer text:\n{footer_text}")

    # Define regex patterns for various section formats
    patterns = [
        {"pattern": re.compile(r'\b(\d{2} \d{2} \d{2}(?:-\d+)?)\b'), "description": "XX XX XX or XX XX XX-1"},
        {"pattern": re.compile(r'\b(\d{6})\b'), "description": "210100 (6 digits)"},
        {"pattern": re.compile(r'\b(\d{6}-\d+)\b'), "description": "210100-1 (6 digits with dash)"},
        {"pattern": re.compile(r'\b(\d{6}\.\d+)\b'), "description": "210100.2 (6 digits with dot)"},
        {"pattern": re.compile(r'\b(\d{6}\.\d+ -?\d?)\b'), "description": "210100.2 - 1 (complex pattern)"},
    ]

    # Process each pattern independently
    for idx, item in enumerate(patterns):
        pattern = item["pattern"]
        description = item["description"]

        # Find matches for this pattern in the footer text
        matches = list(pattern.finditer(footer_text))
        if matches:
            logging.debug(f"Pattern {idx + 1} ({description}) found matches: {[m.group(1) for m in matches]}")
            for match in matches:
                section_text = match.group(1)
                sections[match.start()] = section_text  # Use the position as the key
        else:
            logging.debug(f"Pattern {idx + 1} ({description}) found no matches.")

    # Debug: Log detected sections
    if sections:
        logging.debug(f"Detected sections: {sections}")
    else:
        logging.debug("No sections detected.")

    return sections


# Text wrapping for PDF
def wrap_text_for_pdf(text, max_width):
    from textwrap import wrap
    char_width = 2.5
    max_chars = int(max_width / char_width)
    return wrap(text, width=max_chars)

def convert_text_to_pdf_with_reportlab(text_file, original_pdf, output_pdf):
    """
    Convert a text file to a PDF using reportlab, preserving original page dimensions,
    maintaining line breaks and white space, and ensuring all pages are processed.
    """
    try:
        # Read original PDF to get page dimensions
        original_pdf_reader = PdfReader(original_pdf)
        page_sizes = [
            (float(page.mediabox[2]) - float(page.mediabox[0]),
             float(page.mediabox[3]) - float(page.mediabox[1]))
            for page in original_pdf_reader.pages
        ]
        total_pages = len(page_sizes)

        # Font and spacing settings
        font_size = 8
        line_spacing = font_size + 2

        # Initialize PDF canvas
        c = canvas.Canvas(output_pdf)

        with open(text_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        current_page = 0
        y_position = None

        # Start handling text
        for line in lines:
            if line.strip().startswith("=== Page"):  # Detect new page marker
                # Transition to the next page
                if current_page < total_pages:
                    if current_page > 0:
                        c.showPage()
                    c.setPageSize(page_sizes[current_page])
                    y_position = page_sizes[current_page][1] - 50  # Reset Y position
                    c.setFont("Helvetica", font_size)  # Set font for the new page
                    logging.info(f"Starting new page {current_page + 1} with size {page_sizes[current_page]}")
                    current_page += 1
                else:
                    logging.error("Page index exceeds available page sizes. Skipping.")
                    break
                continue

            if y_position is not None:
                if line.strip() == "":  # Handle empty lines to preserve spacing
                    y_position -= line_spacing
                    continue

                # Wrap text for proper alignment
                wrapped_lines = wrap_text_for_pdf(line, max_width=page_sizes[current_page - 1][0] - 100)
                for wrapped_line in wrapped_lines:
                    if y_position < 50:  # Start a new page if space runs out
                        c.showPage()
                        if current_page < total_pages:
                            c.setPageSize(page_sizes[current_page])
                            c.setFont("Helvetica", font_size)  # Reset font
                            y_position = page_sizes[current_page][1] - 50
                            current_page += 1
                        else:
                            logging.error("No more pages available for text. Skipping remaining lines.")
                            break
                    c.drawString(50, y_position, wrapped_line)
                    y_position -= line_spacing  # Adjust line spacing

        # Finalize and save the PDF
        c.save()
        logging.info(f"Text file successfully converted to PDF: {output_pdf}")
    except Exception as e:
        logging.error(f"Error during text-to-PDF conversion: {e}")
        raise



# Define constants
FALLBACK_COLOR = (0.5, 0, 0.5)  # Purple

# Category priority (Higher priority goes first)
CATEGORY_PRIORITY = ["Competitor", "Manufacturer", "Equipment", "Model", "Universal"]

last_valid_section_name = "Unknown"  # Global variable to track the last valid section name across pages

def find_words_and_highlight(text, word_lists, page_num, pdf_doc, filename):
    """
    Identify and highlight search terms on each page, collect results,
    and include section name tracking across pages.
    """
    global last_valid_section_name
    page = pdf_doc.load_page(page_num)
    normalized_text = re.sub(r'\s+', ' ', text.strip())  # Normalize whitespace
    results = []

    # Debug: Log normalized text for analysis
    logging.debug(f"Page {page_num + 1} text: {normalized_text[:500]}...")

    # Identify sections from the text
    footer_sections = identify_sections(page)
    sorted_sections = sorted(footer_sections.items())
    section_title = sorted_sections[-1][1] if sorted_sections else "Unknown"

    subsection_pattern = re.compile(r'^\s*(\d{1,2}(\.\d+)+)\s+([A-Z][A-Z0-9\s]+)$', re.MULTILINE)
    subsection_matches = list(subsection_pattern.finditer(text))

    if subsection_matches:
        last_section = subsection_matches[-1]
        current_section_name = f"{last_section.group(1)} {last_section.group(3).strip()}"
        last_valid_section_name = current_section_name
    else:
        current_section_name = last_valid_section_name

    for category in CATEGORY_PRIORITY:
        words = word_lists.get(category, [])
        color = highlight_colors.get(category, FALLBACK_COLOR)

        # Debug: Log category and terms
        logging.debug(f"Processing category '{category}' with terms: {words}")

        for word in words:
            pattern = re.compile(r'\b' + re.escape(word) + r'\b', re.IGNORECASE)
            matches = list(re.finditer(pattern, normalized_text))

            if matches:
                word_instances = page.search_for(word)
                for inst in word_instances:
                    highlight = page.add_highlight_annot(inst)
                    highlight.set_colors(stroke=color)
                    highlight.set_opacity(0.6)
                    highlight.update()

                results.append({
                    "Word": word,
                    "Category": category,
                    "Page": page_num + 1,
                    "Section": section_title,
                    "Section Name": current_section_name,
                    "Link": f"/view_pdf?pdf={filename}&page={page_num + 1}&word={word}",
                })

    return results

def process_pdf_file(filepath):
    """
    Process a PDF file to extract text, highlight terms, and track footer-based sections,
    page text-based section names, and 'Acceptable Manufacturers' content.
    Handles both text-based PDFs and image-based PDFs using OCR.
    """
    if not allowed_file(filepath):
        logging.error("Invalid file type. Please provide a PDF file.")
        return None

    load_search_terms()

    # Check if PDF is image-based and perform OCR if needed
    if is_image_based_pdf(filepath):
        try:
            text_file, num_pages = perform_ocr(filepath)
            output_pdf = f"{Path(filepath).stem}_standard.pdf"
            convert_text_to_pdf_with_reportlab(text_file, filepath, output_pdf)
            filepath = output_pdf
        except Exception as e:
            logging.error(f"Error during OCR or text-to-PDF conversion: {e}")
            return None

    # Extract text from the processed or original PDF
    text_by_page = extract_text_from_pdf(filepath)
    pdf_doc = fitz.open(filepath)
    processed_filename = f"Highlighted_{os.path.basename(filepath)}"
    processed_filepath = os.path.join(PROCESSED_FOLDER, processed_filename)
    results_data = {title.replace(" ", "_").lower(): [] for title in search_terms}

    sections = []  # Stores detected sections and section names
    acceptance_sections = []  # Stores 'Acceptable Manufacturers' content
    current_section = None
    start_page = None

    def extract_section_names(page_text):
        """ Extract all section names from the given page text using regex. """
        section_name_pattern = re.compile(r'^\s*(\d+\.\d+)\s*(.+)$', re.MULTILINE)
        matches = section_name_pattern.finditer(page_text)

        section_names = []
        for match in matches:
            section_number = match.group(1)
            section_title = match.group(2).strip()
            combined_name = f"{section_number} {section_title}"
            section_names.append({
                "Section Name": combined_name,
                "Page": match.group(1)
            })
        return section_names

    def extract_acceptance_section(text, section_name, page_num):
        """ Extract 'Acceptable Manufacturers' content with appropriate stops. """
        results = []
        patterns = [
            # Original patterns for A. and 1. Acceptable Manufacturers
            {"start": re.compile(r'(1\.\s+Acceptable\s+Manufacturers:?)', re.IGNORECASE), "stop": re.compile(r'(?=\b2\.)')},
            {"start": re.compile(r'(A\.\s+Acceptable\s+Manufacturers:?)', re.IGNORECASE), "stop": re.compile(r'(?=\bB\.)')},
            # Additional patterns for Manufacturers without "Acceptable"
            {"start": re.compile(r'(1\.\s+Manufacturers:?)', re.IGNORECASE), "stop": re.compile(r'(?=\b2\.)')},
            {"start": re.compile(r'(A\.\s+Manufacturers:?)', re.IGNORECASE), "stop": re.compile(r'(?=\bB\.)')}
        ]
        for pattern in patterns:
            for match in pattern["start"].finditer(text):
                start_pos = match.end()
                sliced_text = text[start_pos:]
                stop_match = pattern["stop"].search(sliced_text)
                extracted_text = sliced_text[:stop_match.start()].strip() if stop_match else sliced_text.strip()
                full_text = match.group(1) + "\n" + extracted_text
                results.append({
                    "Section Name": section_name,
                    "text": full_text,
                    "Page": page_num + 1,
                    "Link": f"/view_pdf?pdf={processed_filename}&page={page_num + 1}&word=Acceptable Manufacturers"
                })
        return results

    def process_page(page_num, page_text):
        """ Process a single page to extract sections and highlight terms. """
        nonlocal current_section, start_page

        if not page_text.strip():  # Skip pages with no text
            logging.warning(f"Page {page_num + 1} has no valid text. Skipping.")
            return

        # Extract sections from footer
        footer_sections = identify_sections(pdf_doc[page_num])
        sorted_footer_sections = sorted(footer_sections.items())
        detected_footer_section = sorted_footer_sections[-1][1] if sorted_footer_sections else "Unknown"

        # Extract section names
        detected_section_names = extract_section_names(page_text)

        # Handle footer-based section changes
        if detected_footer_section != current_section:
            if current_section is not None and start_page is not None:
                sections.append({
                    "Section": current_section,
                    "Section Name": None,
                    "Start Page": start_page + 1,
                    "Stop Page": page_num,
                    "Link": f"/view_pdf?pdf={processed_filename}&page={start_page + 1}&word={current_section}",
                    "Download Link": f"/download_section?start_page={start_page + 1}&stop_page={page_num}&section_name={current_section}&filename={processed_filename}"
                })
            current_section = detected_footer_section
            start_page = page_num

        # Add all detected section names
        for section_name in detected_section_names:
            sections.append({
                "Section": detected_footer_section,
                "Section Name": section_name["Section Name"],
                "Start Page": page_num + 1,
                "Stop Page": page_num + 1,
                "Link": f"/view_pdf?pdf={processed_filename}&page={page_num + 1}&word={section_name['Section Name']}",
                "Download Link": f"/download_section?start_page={start_page + 1}&stop_page={page_num}&section_name={current_section}&filename={processed_filename}"
            })

        # Extract 'Acceptable Manufacturers'
        acceptance_matches = extract_acceptance_section(
            page_text,
            detected_section_names[-1]["Section Name"] if detected_section_names else "Unknown",
            page_num
        )
        acceptance_sections.extend(acceptance_matches)

        # Highlight terms and collect results
        page_results = find_words_and_highlight(page_text, search_terms, page_num, pdf_doc, processed_filename)
        for result in page_results:
            category_key = result['Category'].replace(" ", "_").lower()
            results_data[category_key].append(result)

    try:
        for page_num, page_text in enumerate(text_by_page):
            process_page(page_num, page_text)

        # Close the last footer section
        if current_section is not None and start_page is not None:
            sections.append({
                "Section": current_section,
                "Section Name": None,
                "Start Page": start_page + 1,
                "Stop Page": len(text_by_page),
                "Link": f"/view_pdf?pdf={processed_filename}&page={start_page + 1}",
                "Download Link": f"/download_section?start_page={start_page + 1}&stop_page={page_num}&section_name={current_section}&filename={processed_filename}"
            })

    except Exception as e:
        logging.error(f"Error during page processing: {e}")
        return None

    # Filter out invalid sections
    filtered_sections = [s for s in sections if s["Section Name"] is not None]

    # Save the highlighted PDF
    try:
        pdf_doc.save(processed_filepath)
        pdf_doc.close()
    except Exception as e:
        logging.error(f"Error saving highlighted PDF: {e}")
        return None

    return {
        "results": results_data,
        "sections": filtered_sections,
        "acceptance_sections": acceptance_sections,
        "filename": processed_filename,
        "total_pages": len(text_by_page),
    }



def output_results_as_json(results_data, sections, acceptance_sections, filename, total_pages):
    """
    Output results as JSON without saving to a file.
    Includes section start and stop pages, as well as 'Acceptable Manufacturers' sections.
    """
    output = {
        "filename": filename,
        "total_pages": total_pages,
        "results": results_data,
        "sections": sections,  # Add sections to the output
        "acceptance_sections": acceptance_sections  # Add 'Acceptable Manufacturers' sections to the output
    }
    json_output = json.dumps(output, indent=4)
    print("\n--- JSON Output ---")
    print(json_output)
    return output


def main():
    """
    Standalone script entry point.
    Prompts the user for a PDF file path, processes it, and outputs JSON results.
    """
    # Prompt user for the PDF file path
    file_path = input("Enter the path to your PDF file: ").strip()

    if not os.path.isfile(file_path):
        print("Error: File not found. Please check the file path and try again.")
        return

    print("Processing the PDF file...")

    # Process the PDF file
    results = process_pdf_file(file_path)

    if results:  # If processing was successful
        # Display the JSON output
        output_results_as_json(
            results_data=results["results"],
            sections=results["sections"],
            acceptance_sections=results["acceptance_sections"],  # Pass 'Acceptable Manufacturers'
            filename=results["filename"],
            total_pages=results["total_pages"],
        )
    else:
        print("Processing failed. No results to display.")


if __name__ == "__main__":
    main()


























