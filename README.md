# Rep-Rakbank-POC-Kafka-Webhook-String
Rakbank POC - Python AVRO String Processing Webhook Service for Kafka Integration
# 🐍 Rakbank POC Kafka Webhook Python Service

Python AVRO String Processing Webhook Service for Kafka Integration

## Features
- ✅ **FastAPI** - Modern, fast web framework
- ✅ **AVRO Union Type Parser** - Regex-based string extraction  
- ✅ **Transaction Storage** - In-memory last 10 transactions
- ✅ **Error Handling** - Graceful fallback processing
- ✅ **Auto Documentation** - Available at `/docs`
- ✅ **Health Monitoring** - Built-in health checks
- ✅ **Azure Ready** - Direct deployment support

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/webhook/user-transactions` | Receive transaction data from Kafka |
| GET | `/webhook/user-transactions` | Get statistics and last 10 transactions |
| GET | `/health` | Health check endpoint |
| POST | `/webhook/reset` | Reset transaction counters |
| GET | `/` | API information |
| GET | `/docs` | Auto-generated API documentation |

## Supported Data Formats

### Primary: AVRO Union Type
```json
{"authorizer_usrnbr":{"string":"1"},"creat_usrnbr":{"string":"123"},"creat_time":{"string":"2025-07-10T07:00:00"},"data":{"string":"transaction data"},"usrname":{"string":"USER_123"}}
