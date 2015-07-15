-module(mem3_httpd_handlers).

-export([url_handler/1, db_handler/1, design_handler/1]).

url_handler(<<"_membership">>) -> fun mem3_httpd:handle_membership_req/1;
url_handler(_) -> no_match.

db_handler(<<"_shards">>) -> fun mem3_httpd:handle_shards_req/2;
db_handler(_) -> no_match.

design_handler(_) -> no_match.
