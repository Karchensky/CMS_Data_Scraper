from flask import Flask, render_template, request, send_file, make_response, send_from_directory
import io
import pandas as pd
from cms_data_scraper import cms_data_scraper
import os

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/result', methods=['POST'])
def result():
    uuid_list = request.form.get('uuid_list')
    uuid_list = [uuid.strip() for uuid in uuid_list.split(',')]

    # Combine CMS data for each UUID into a single DataFrame
    cms_data_combined = pd.DataFrame()
    for uuid in uuid_list:
        scraper = cms_data_scraper(uuid)
        cms_data = scraper.scraper_cms()
        cms_data_combined = pd.concat([cms_data_combined, cms_data], ignore_index=True)

    # Generate CSV file from the combined data
    csv_data = cms_data_combined.to_csv(index=False)

    # Create an in-memory file object
    csv_io = io.StringIO()
    csv_io.write(csv_data)
    csv_io.seek(0)

    # Set response headers to download the file
    response = make_response(csv_io.getvalue())
    response.headers['Content-Type'] = 'text/csv'
    response.headers['Content-Disposition'] = 'attachment; filename=cms_data.csv'

    return response

# Route for serving static files (styles.css, images, etc.)
@app.route('/static/<path:filename>')
def static_files(filename):
    root_dir = os.path.dirname(os.path.abspath(__file__))
    return send_from_directory(os.path.join(root_dir, 'static'), filename)

if __name__ == '__main__':
    app.run()