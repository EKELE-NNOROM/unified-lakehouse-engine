-- Seed event_metrics (all 25 rows from sample_events.json)
-- Apply after sql/schemas/init.sql on StarRocks analytics database

INSERT INTO event_metrics (event_id, event_type, metric_value, event_date) VALUES
('evt-001', 'order.placed', 249, '2026-05-25'),
('evt-002', 'order.shipped', 249, '2026-05-25'),
('evt-003', 'order.placed', 89, '2026-05-25'),
('evt-004', 'click', 1, '2026-05-25'),
('evt-005', 'page.view', 1, '2026-05-25'),
('evt-006', 'order.placed', 512, '2026-05-25'),
('evt-007', 'order.delivered', 249, '2026-05-26'),
('evt-008', 'order.shipped', 512, '2026-05-25'),
('evt-009', 'click', 1, '2026-05-25'),
('evt-010', 'order.placed', 34, '2026-05-25'),
('evt-011', 'page.view', 1, '2026-05-25'),
('evt-012', 'order.placed', 1200, '2026-05-25'),
('evt-013', 'order.shipped', 1200, '2026-05-26'),
('evt-014', 'click', 1, '2026-05-25'),
('evt-015', 'order.placed', 67, '2026-05-25'),
('evt-016', 'page.view', 1, '2026-05-25'),
('evt-017', 'order.placed', 445, '2026-05-25'),
('evt-018', 'order.delivered', 512, '2026-05-27'),
('evt-019', 'click', 1, '2026-05-25'),
('evt-020', 'order.placed', 199, '2026-05-25'),
('evt-021', 'order.shipped', 34, '2026-05-26'),
('evt-022', 'page.view', 1, '2026-05-25'),
('evt-023', 'order.placed', 78, '2026-05-25'),
('evt-024', 'order.placed', 320, '2026-05-25'),
('evt-025', 'order.delivered', 1200, '2026-05-27');
