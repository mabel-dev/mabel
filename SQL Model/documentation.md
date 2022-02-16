# Naming things

Dbt has a few different recommendations for mart models. They suggest using two options for prefixes with these models:

fct _<verb>
dim_<noun>

The dbt documentation defines fct tables as, “a tall, narrow table representing real-world processes that have occurred or are occurring. The heart of these models is usually an immutable event stream: sessions, transactions, orders, stories, votes”.

In contrast, they define dim tables as, “ a wide, short table where each row is a person, place, or thing; the ultimate source of truth when identifying and describing entities of the organization. They are mutable, though slowly changing: customers, products, candidates, buildings, employees”.
