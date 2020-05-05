
This directory is focused on supporting REST APIs related to visualizing data that is requested through the UI.  Some of the queries are based on group-by and aggregation and others will produce lists of records.

This area is a WORK IN PROGRESS.

At present there are 2 main files:

- REST_APIs_for_queries.py: Holds the REST APIs and does some checking for invalid input arguments

- basic_query_processing.py: Holds parameterized SQL queries that support the REST APIs

Here is the list of functions and subfunctions that will be implemented for
"Panel 1", the overview panel.  The listing shows the main function name, the
subfunction name, and then after the ":" it shows the parameters needed for them.

To illustrate, a couple of curl calls for the first function are as follows:

- curl --url http://127.0.0.1:5000/claims_decided/'?subfunction=total_this_year&today=2020-04-10'

- curl --url http://127.0.0.1:5000/claims_decided/'?subfunction=total_in_period&today=2020-04-10&months_before=2'

- curl --url http://127.0.0.1:5000/claims_decided/'?subfunction=TAT_gt_n_in_period&today=2020-04-10&months_before=2&biz_days_count=10'



Here are the functions:

- claims_decided            : today
  - this_year             
  - this_month
  - total_in_period        : biz_days_before XOR months_before
  - TAT_gt_n_in_period     : biz_days_before XOR months_before, biz_days_count

(The function/subfunctions above have been implemented.  The ones below are in progress.)

- claims_returned_to_work   : today, months_before
  - total
  - duration_gt_x_percent  : percent

- decision_inventory        : today
  - total
  - pending_gt_n_days      : biz_days_count

- pay_out_inventory             : today
  - total
  - count_with_missed_touches  

- claims_received               : today
  - this_year
  - this_month

Additional functions/subfunctions will be developed for the other panels.

