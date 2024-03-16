import xml.etree.ElementTree as ET
import json

def parse_xml_report(file_path):
    # Parse the entire XML file
    tree = ET.parse(file_path)
    root = tree.getroot()

    # Assuming the first testsuite element is the one we're interested in
    testsuite = root.find('testsuite')

    # Extract 'failures' and 'tests' attributes
    failures = int(testsuite.attrib['failures'])
    total_tests = int(testsuite.attrib['tests'])
    passed = total_tests - failures

    # Construct the JSON structure
    report = {
        "schemaVersion": 1,
        "label": "tests",
        "message": f"{passed} passed, {failures} failed",
        "color": "red"
    }

    return report

def write_json_report(report, output_file):
    # Write the JSON to report.json
    with open(output_file, 'w') as json_file:
        json.dump(report, json_file, indent=4)
    print(f"{output_file} has been successfully created.")

if __name__ == "__main__":
    report = parse_xml_report('report.xml')
    write_json_report(report, 'report.json')

