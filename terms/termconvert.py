import pandas as pd
import json

def convert_to_json(input_excel, output_json):
    # Read the Excel file
    df = pd.read_excel(input_excel)
    
    # Define the categories and their corresponding colors
    categories = {
        "Equipment": "#FF5733",
        "Manufacturer": "#33CFFF",
        "Model": "#FFC300",
        "Universal Terms": "#9C33FF",
        "Competitor": "#C70039",
    }
    
    # Create the JSON structure
    json_output = []
    for category, color in categories.items():
        # Filter terms under the category
        terms = df[category].dropna().tolist() if category in df.columns else []
        
        # Add to JSON structure
        json_output.append({
            "title": category,
            "color": color,
            "terms": terms
        })
    
    # Save the JSON structure to a file
    with open(output_json, "w") as json_file:
        json.dump(json_output, json_file, indent=4)

# Input and output file paths
input_excel = "UEP Terms.xlsx"
output_json = "UEP_Terms.json"

# Run the conversion
convert_to_json(input_excel, output_json)

print(f"JSON file has been created at: {output_json}")
