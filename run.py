from db import get_is_brand_portal, query_text_connection, query_db, query_text_session
from tutorial import my_flow_error

my_flow_error()

s = get_is_brand_portal()
print(s)

s = query_text_session("SELECT * FROM isBrandPortal")
print(s)

s = query_text_connection("SELECT * FROM isBrandPortal")
print(s)

try:
    s = query_text_connection("SELECT * FROM isBrandPortal2")
    print(s)
except Exception as e:
    print(e)

try:
    s = query_text_session("SELECT * FROM isBrandPortal2")
    print(s)
except Exception as e:
    print(e)

s = query_db("SELECT * FROM isBrandPortal")
print(s.all()[0])
