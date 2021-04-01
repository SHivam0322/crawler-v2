cd crawler\indeed\ &
py fetch_html.py &
py parse_html.py &
cd.. &
cd crawler\common &
py mongo_jobs_save.py