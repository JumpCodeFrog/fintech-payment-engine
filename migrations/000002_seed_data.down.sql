DELETE FROM accounts
WHERE id IN (
    'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaa0001',
    'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaa0002',
    'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbb0001',
    'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbb0002'
);

DELETE FROM users
WHERE id IN (
    '11111111-1111-4111-8111-111111111111',
    '22222222-2222-4222-8222-222222222222'
);

DROP TABLE IF EXISTS users;
