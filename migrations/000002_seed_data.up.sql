CREATE TABLE users (
    id UUID PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO users (id, name)
VALUES
    ('11111111-1111-4111-8111-111111111111', 'Alice'),
    ('22222222-2222-4222-8222-222222222222', 'Bob')
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;

INSERT INTO accounts (
    id, user_id, currency, balance, frozen_balance, version
)
VALUES
    ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaa0001', '11111111-1111-4111-8111-111111111111', 'USD', 10000.0000, 0.0000, 1),
    ('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaa0002', '11111111-1111-4111-8111-111111111111', 'EUR', 10000.0000, 0.0000, 1),
    ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbb0001', '22222222-2222-4222-8222-222222222222', 'USD', 5000.0000, 0.0000, 1),
    ('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbb0002', '22222222-2222-4222-8222-222222222222', 'EUR', 5000.0000, 0.0000, 1)
ON CONFLICT (id) DO UPDATE SET
    user_id = EXCLUDED.user_id,
    currency = EXCLUDED.currency,
    balance = EXCLUDED.balance,
    frozen_balance = EXCLUDED.frozen_balance,
    version = EXCLUDED.version,
    updated_at = NOW();
