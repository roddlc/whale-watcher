"""Debug script to inspect filing index HTML structure."""

import requests

# Fetch Berkshire's recent filing -index.html file (contains document table with Type column)
url = "https://www.sec.gov/Archives/edgar/data/1067983/000119312525282901/0001193125-25-282901-index.html"

headers = {
    'User-Agent': 'WhaleWatcher/1.0 (parade.gazebos.4g@icloud.com)',
}

response = requests.get(url, headers=headers)
html = response.text

# Save to file for inspection
with open('local/filing_index_sample.html', 'w') as f:
    f.write(html)

print("Saved HTML to local/filing_index_sample.html")
print(f"HTML length: {len(html)} bytes")

# Try to find table structure
import re

# Look for table rows
table_row_pattern = r'<tr[^>]*>(.*?)</tr>'
matches = re.findall(table_row_pattern, html, re.IGNORECASE | re.DOTALL)

print(f"\nFound {len(matches)} table rows")
print("\nFirst few rows:")
for i, match in enumerate(matches[:5]):
    print(f"\n--- Row {i} ---")
    print(match[:500])  # Print first 500 chars
