-- Seed event_segments (warehouse enrichment for federated joins)
-- Apply after sql/schemas/init.sql on Redshift public schema

INSERT INTO event_segments (event_id, segment, score) VALUES
('evt-001', 'high_value', 0.92),
('evt-002', 'high_value', 0.88),
('evt-003', 'standard', 0.45),
('evt-004', 'engagement', 0.30),
('evt-005', 'engagement', 0.25),
('evt-006', 'high_value', 0.95),
('evt-007', 'high_value', 0.90),
('evt-008', 'high_value', 0.87),
('evt-009', 'engagement', 0.28),
('evt-010', 'standard', 0.40),
('evt-011', 'engagement', 0.22),
('evt-012', 'high_value', 0.98),
('evt-013', 'high_value', 0.91),
('evt-014', 'engagement', 0.32),
('evt-015', 'standard', 0.42),
('evt-016', 'engagement', 0.20),
('evt-017', 'high_value', 0.85),
('evt-018', 'high_value', 0.89),
('evt-019', 'engagement', 0.35),
('evt-020', 'standard', 0.55),
('evt-021', 'standard', 0.38),
('evt-022', 'engagement', 0.18),
('evt-023', 'standard', 0.48),
('evt-024', 'high_value', 0.75),
('evt-025', 'high_value', 0.93);
