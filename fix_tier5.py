with open('worker_pool.py', 'r') as f:
    content = f.read()

# Fix 1: Move variable extraction BEFORE the file_processing block
old_section = '''        # --- Handle file upload jobs ---
        if job_type == 'file_processing':'''

new_section = '''        # Extract common variables needed for both paths
        content_url   = sub['content_url'] if sub else ''
        creator_id    = str(sub['creator_id']) if sub else ''
        thumbnail_url = sub['content_preview_url'] or '' if sub else ''

        # --- Handle file upload jobs ---
        if job_type == 'file_processing':'''

content = content.replace(old_section, new_section)

# Fix 2: Fix the duplicate function issue - keep only the first one
# Find and remove the duplicate at the end (after main loop)
import re
# Remove everything after the main loop that contains duplicate function
content = re.sub(
    r'\n\ndef get_cached_fingerprint_from_file\(file_path: str\) -> tuple:.*?return None, None\n\n# Alias.*$',
    '',
    content,
    flags=re.DOTALL
)

with open('worker_pool.py', 'w') as f:
    f.write(content)
print("✅ Tier-5 fixes applied")
