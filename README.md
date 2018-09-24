## Mapred

A sample Mapreduce code finding longest user sessions. Assumes that data has a comma-separated CSV format in the form *username, session_id, event_type,event_time* where *event_type* is one of *['begin','end']* and event_time is the time of the event in the form *"yyyy-MM-ddTHH:mm:ss"*.

Can also serve as an example for composite keys and values.