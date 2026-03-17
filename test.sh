#!/bin/bash
curl -s -o /dev/null -w "Status: %{http_code}\n" -X POST http://localhost:8100/api/events \
  -H "Content-Type: application/json" \
  -H "X-API-Key: test-key-1" \
  -d '{
    "eventId": "550e8400-e29b-41d4-a716-446655440001",
    "eventType": "page_view",
    "timestamp": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'",
    "userId": "user_demo_123",
    "sessionId": "sess_999",
    "url": "http://localhost/test",
    "service": "frontend-app",
    "statusCode": 200
  }'
