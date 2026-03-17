$dateStr = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ssZ")
$body = @"
{
  "eventId": "550e8400-e29b-41d4-a716-446655440001",
  "eventType": "page_view",
  "timestamp": "$dateStr",
  "userId": "user_demo_123",
  "sessionId": "sess_999",
  "url": "http://localhost/test",
  "service": "frontend-app",
  "statusCode": 200
}
"@

try {
    $response = Invoke-WebRequest -Uri "http://localhost:8100/api/events" -Method Post -Headers @{"Content-Type"="application/json"; "X-API-Key"="test-key-1"} -Body $body -SkipHttpErrorCheck
    Write-Output "Status: $($response.StatusCode)"
} catch {
    Write-Error $_
}
