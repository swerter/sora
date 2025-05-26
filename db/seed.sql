-- Test user for development "user@domain.com" with password "password"
INSERT into users (username, password) values ('user@domain.com', '$2a$10$59jW86pmlBLK2CF.hqmNpOWDPFRPKLWm4u6mpP/p.q1gtH3P0sqyK') ON CONFLICT (username) DO NOTHING;
-- Add another user, password created with iex>  Bcrypt.hash_pwd_salt("mysecret")
INSERT into users (username, password) values ('test@test.net', '$2b$12$s4tZlMwI3JqJN0gVuGfRxOYA3cWZsCxSnioD5jgwODgJ6S5u6Kdt6') ON CONFLICT (username) DO NOTHING;
