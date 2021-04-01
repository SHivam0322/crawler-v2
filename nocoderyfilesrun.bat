
cd crawler\nocodery\ &
py fetch_html.py &
py fetch_job_detail.py &
py parse_html.py &
cd.. &
cd crawler\common &
py mongo_jobs_save.py