from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import json
import re
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Rakbank POC Kafka Webhook Service",
    description="Python AVRO String Processing Webhook Service for Kafka Integration",
    version="1.0.0"
)

# Data Models
class TransactionData(BaseModel):
    authorizer_usrnbr: Optional[str] = Field(default="UNKNOWN")
    creat_usrnbr: Optional[str] = Field(default="UNKNOWN") 
    creat_time: Optional[str] = Field(default="")
    data: Optional[str] = Field(default="")
    usrname: Optional[str] = Field(default="UNKNOWN")
    received_at: datetime = Field(default_factory=datetime.now)

class TransactionResponse(BaseModel):
    total_count: int
    last_10_transactions: List[TransactionData]
    last_updated: datetime
    poc_status: str = "active"
    processing_mode: str = "python_avro"

# In-memory storage
transaction_counter = 0
transaction_storage: List[TransactionData] = []

# AVRO String Parser
class AvroStringParser:
    
    @staticmethod
    def parse_avro_string(raw_data: str) -> TransactionData:
        """Parse AVRO Union Type string format"""
        try:
            logger.info(f"Parsing AVRO string: {raw_data[:100]}...")
            
            # Extract values from AVRO union types
            authorizer_usrnbr = AvroStringParser._extract_string_value(raw_data, "authorizer_usrnbr")
            creat_usrnbr = AvroStringParser._extract_string_value(raw_data, "creat_usrnbr")
            creat_time = AvroStringParser._extract_string_value(raw_data, "creat_time")
            data = AvroStringParser._extract_string_value(raw_data, "data")
            usrname = AvroStringParser._extract_string_value(raw_data, "usrname")
            
            transaction = TransactionData(
                authorizer_usrnbr=authorizer_usrnbr,
                creat_usrnbr=creat_usrnbr,
                creat_time=creat_time,
                data=data,
                usrname=usrname
            )
            
            logger.info(f"Successfully parsed transaction from user: {usrname}")
            return transaction
            
        except Exception as e:
            logger.warning(f"Parse failed: {e}")
            return AvroStringParser._create_fallback_transaction(raw_data, str(e))
    
    @staticmethod
    def _extract_string_value(avro_string: str, field_name: str) -> str:
        """Extract string value from AVRO union type format"""
        
        # Pattern 1: "field": {"string": "value"}
        pattern1 = rf'"{field_name}"\s*:\s*{{\s*"string"\s*:\s*"([^"]+)"\s*}}'
        match = re.search(pattern1, avro_string)
        if match:
            return match.group(1)
        
        # Pattern 2: "field": "value" (direct string)
        pattern2 = rf'"{field_name}"\s*:\s*"([^"]+)"'
        match = re.search(pattern2, avro_string)
        if match:
            return match.group(1)
            
        return "UNKNOWN"
    
    @staticmethod
    def _create_fallback_transaction(raw_data: str, error_msg: str) -> TransactionData:
        """Create fallback transaction when parsing fails"""
        return TransactionData(
            authorizer_usrnbr="PARSE_ERROR",
            creat_usrnbr="PARSE_ERROR", 
            creat_time=datetime.now().isoformat(),
            data=raw_data[:200] + ("..." if len(raw_data) > 200 else ""),
            usrname=f"ERROR: {error_msg[:50]}"
        )

# API Endpoints
@app.post("/webhook/user-transactions")
async def receive_transaction(request: Request):
    """Receive transaction data from Kafka connector"""
    global transaction_counter, transaction_storage
    
    try:
        # Get raw body as string
        raw_body = await request.body()
        raw_data = raw_body.decode('utf-8')
        
        logger.info(f"Received transaction data: {len(raw_data)} chars")
        
        # Parse AVRO string
        transaction = AvroStringParser.parse_avro_string(raw_data)
        
        # Store transaction
        transaction_storage.append(transaction)
        transaction_counter += 1
        
        # Keep only last 10 transactions
        if len(transaction_storage) > 10:
            transaction_storage = transaction_storage[-10:]
        
        logger.info(f"Transaction processed successfully. Total count: {transaction_counter}")
        
        return {
            "status": "success",
            "message": "Transaction received successfully",
            "count": transaction_counter,
            "poc_id": transaction_counter,
            "user": transaction.usrname,
            "received_at": transaction.received_at.isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error processing transaction: {e}")
        raise HTTPException(status_code=400, detail=f"Failed to process transaction: {str(e)}")

@app.get("/webhook/user-transactions")
async def get_transactions():
    """Get transaction statistics and last 10 transactions"""
    global transaction_counter, transaction_storage
    
    # Reverse list to show newest first
    last_transactions = list(reversed(transaction_storage))
    
    response = TransactionResponse(
        total_count=transaction_counter,
        last_10_transactions=last_transactions,
        last_updated=datetime.now()
    )
    
    return response

@app.post("/webhook/reset")
async def reset_counters():
    """Reset transaction counters and storage"""
    global transaction_counter, transaction_storage
    
    transaction_counter = 0
    transaction_storage = []
    
    logger.info("Transaction counters reset")
    
    return {
        "status": "success",
        "message": "Counters reset successfully",
        "timestamp": datetime.now().isoformat()
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "rakbank-poc-kafka-webhook",
        "version": "1.0.0",
        "timestamp": datetime.now().isoformat(),
        "total_transactions": transaction_counter
    }

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Rakbank POC Kafka Webhook Service",
        "version": "1.0.0",
        "processing_mode": "python_avro_string",
        "endpoints": {
            "webhook_post": "/webhook/user-transactions",
            "webhook_get": "/webhook/user-transactions", 
            "health": "/health",
            "reset": "/webhook/reset",
            "docs": "/docs"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
