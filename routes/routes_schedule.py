"""
routes_schedule.py
==================
Flask routes for HVAC schedule extraction.

Templates required in /templates:
    - schedule_upload.html
    - schedule_results.html
    - schedule_processing.html

Usage:
    from routes_schedule import schedule_bp
    app.register_blueprint(schedule_bp)
"""

from flask import Blueprint, request, jsonify, render_template, send_file, Response
from io import BytesIO

from helpers.helpers_schedule101 import (
    process_upload,
    get_job_status,
    get_job,
    get_result_excel_bytes,
    get_result_json
)

schedule_bp = Blueprint('schedule', __name__)


# =============================================================================
# PAGE ROUTES
# =============================================================================

@schedule_bp.route('/schedule')
def schedule_upload_page():
    """Render the upload page."""
    return render_template('schedule_upload.html')


@schedule_bp.route('/schedule/results/<job_id>')
def schedule_results_page(job_id):
    """Render the results page for a completed job."""
    job = get_job(job_id)
    
    if not job:
        return jsonify({"error": "Job not found"}), 404
    
    if job.status in ("completed", "completed_with_errors"):
        title = f"Equipment Schedules - {job.filename}"
        return render_template('schedule_results.html', result=job, title=title)
    
    # Job still processing
    status = get_job_status(job_id)
    return render_template('schedule_processing.html', job_id=job_id, status=status)


# =============================================================================
# API ROUTES
# =============================================================================

@schedule_bp.route('/api/schedule/upload', methods=['POST'])
def api_schedule_upload():
    """
    Upload a PDF for schedule extraction.
    
    POST /api/schedule/upload
    Content-Type: multipart/form-data
    Body: file=<pdf_file>
    
    Returns: { success, job_id, filename, status, message }
    """
    if 'file' not in request.files:
        return jsonify({"success": False, "error": "No file provided"}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({"success": False, "error": "No file selected"}), 400
    
    if not file.filename.lower().endswith('.pdf'):
        return jsonify({"success": False, "error": "Only PDF files are supported"}), 400
    
    result = process_upload(file, async_mode=True)
    return jsonify(result)


@schedule_bp.route('/api/schedule/status/<job_id>')
def api_schedule_status(job_id):
    """
    Get status of an extraction job.
    
    GET /api/schedule/status/<job_id>
    
    Returns: { job_id, status, progress, filename, page_count, tables_found, ... }
    """
    status = get_job_status(job_id)
    if status:
        return jsonify(status)
    return jsonify({"error": "Job not found"}), 404


@schedule_bp.route('/api/schedule/result/<job_id>')
def api_schedule_result(job_id):
    """
    Get full extraction result as JSON.
    
    GET /api/schedule/result/<job_id>
    
    Returns: Full ExtractionResult as JSON
    """
    job = get_job(job_id)
    if job:
        if job.status in ("completed", "completed_with_errors"):
            return jsonify(job.to_dict())
        else:
            return jsonify({"error": "Job not complete", "status": job.status}), 202
    return jsonify({"error": "Job not found"}), 404


@schedule_bp.route('/api/schedule/submit', methods=['POST'])
def api_schedule_submit():
    """
    Submit selected equipment for quote.
    
    POST /api/schedule/submit
    Content-Type: application/json
    Body: { equipment: [...], job_id: "..." }
    
    Returns: { success, message, quote_id }
    """
    data = request.get_json()
    if not data:
        return jsonify({"success": False, "error": "No data provided"}), 400
    
    equipment = data.get('equipment', [])
    job_id = data.get('job_id', '')
    
    if not equipment:
        return jsonify({"success": False, "error": "No equipment selected"}), 400
    
    # TODO: Integrate with your quote/ordering system
    
    return jsonify({
        "success": True,
        "message": f"Received {len(equipment)} equipment items",
        "equipment_count": len(equipment),
        "job_id": job_id
    })


# =============================================================================
# DOWNLOAD ROUTES
# =============================================================================

@schedule_bp.route('/schedule/download/<job_id>/excel')
def download_excel(job_id):
    """Download extraction results as Excel file."""
    excel_bytes = get_result_excel_bytes(job_id)
    if excel_bytes:
        job = get_job(job_id)
        filename = f"{job.filename.rsplit('.', 1)[0]}_schedules.xlsx" if job else f"{job_id}_schedules.xlsx"
        return send_file(
            BytesIO(excel_bytes),
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename
        )
    return jsonify({"error": "Job not found or not complete"}), 404


@schedule_bp.route('/schedule/download/<job_id>/json')
def download_json(job_id):
    """Download extraction results as JSON file."""
    json_str = get_result_json(job_id)
    if json_str:
        job = get_job(job_id)
        filename = f"{job.filename.rsplit('.', 1)[0]}_schedules.json" if job else f"{job_id}_schedules.json"
        return Response(
            json_str,
            mimetype='application/json',
            headers={'Content-Disposition': f'attachment; filename="{filename}"'}
        )
    return jsonify({"error": "Job not found or not complete"}), 404
