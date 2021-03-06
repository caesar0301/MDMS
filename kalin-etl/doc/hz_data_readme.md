HangZhou Data Description
--------------------------

This data set contains user web-browsing logs in 19 days, across two months,
in August and October, 2012. The network topology covers the main areas of
Hangzhou City and Wenzhou City, Zhejiang Province.

Data Columns
-------------

This folder maintains the set AFTER data cleansing and formatting. Each set
have 27 independent columns to describe user web-browsing activities.

* ttime (double): timestamp issuing a web request
* dtime (double): timestamp ending a request or dumping this log
* BS (long): signature of individual base stations (LAC*10^6 + CI)
* IMSI (string): user IMSI signature
* mobile_type (string): signature of mobile client type
* dest_ip (long): destination IP address
* dest_port (int): destination TCP port
* success (long): indicating if the web request succeeded
* failure_cause (string): reason of web request failure
* response_time (long): time delay from request to the first byte of response
* host (string): host name of web request
* content_length (long): content-length field of HTTP header
* retransfer_count (long): the number of retransmission
* packets (long): the number of network packets
* status_code (int): HTTP status code
* web_volume (long): byte number of transfered web request
* content_type (string): content-type field of HTTP header
* user_agent (string): MD5 value of user-agent field of HTTP header
* is_mobile (int): if the client is mobile device
* e_gprs (int): E_GPRS mode indicator
* umts_tdd (int): UMTS/TDD mode indicator
* ICP (long): classification of Internet Content Providers, e.g., Netease
* SC (string): service classification, e.g., video, music.
* URI (string): Uniform resource identifier
* OS (string): operating system type
* LON (double): latitude of base station location
* LAT (double): longitude of base station location