"""
Kafka Producer for LiveInsight+ Retail System
Streams retail transaction data to Kafka topic: retail.transactions
Replaces socket-based streaming with production-ready Kafka ingestion
"""

import time
import json
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class RetailKafkaProducer:
    """High-throughput Kafka producer for retail transactions"""
    
    def __init__(self, bootstrap_servers='localhost:9092', topic='retail.transactions'):
        self.topic = topic
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',  # Wait for all replicas
            compression_type='lz4',  # Fast compression
            max_in_flight_requests_per_connection=5,
            retries=3,
            linger_ms=10,  # Batch messages for 10ms
            batch_size=16384  # 16KB batches
        )
        logger.info(f"✅ Kafka producer initialized: {bootstrap_servers}")
        logger.info(f"📡 Target topic: {topic}")
    
    def send_data(self, file_path, delay=0.1, loop=False):
        """
        Stream CSV data to Kafka topic
        
        Args:
            file_path: Path to retail_data_bangalore.csv
            delay: Delay between messages (seconds)
            loop: If True, restart from beginning when file ends
        """
        try:
            df = pd.read_csv(file_path)
            logger.info(f"📂 Loaded {len(df)} transactions from {file_path}")
            
            total_sent = 0
            start_time = time.time()
            
            while True:
                for idx, row in df.iterrows():
                    # Prepare transaction message
                    transaction = {
                        'TransactionID': str(row['TransactionID']),
                        'Timestamp': pd.to_datetime(row['Timestamp']).strftime("%Y-%m-%d %H:%M:%S"),
                        'StoreName': str(row['StoreName']),
                        'BranchName': str(row['BranchName']),
                        'BranchID': int(float(row['BranchID'])),
                        'TotalBranches': int(float(row['TotalBranches'])),
                        'CustomerID': int(float(row['CustomerID'])),
                        'Category': str(row['Category']),
                        'Product': str(row['Product']),
                        'Quantity': int(float(row['Quantity'])),
                        'UnitPrice': float(row['UnitPrice']),
                        'TotalAmount': float(row['TotalAmount']),
                        'Discount': float(row['Discount']),
                        'FinalAmount': float(row['FinalAmount']),
                        'LoyaltyPoints': float(row['LoyaltyPoints']),
                        'PaymentType': str(row['PaymentType'])
                    }
                    
                    # Send to Kafka
                    future = self.producer.send(self.topic, value=transaction)
                    
                    # Non-blocking callback
                    future.add_callback(self._on_success, transaction['TransactionID'])
                    future.add_errback(self._on_error)
                    
                    total_sent += 1
                    
                    # Log progress every 100 messages
                    if total_sent % 100 == 0:
                        elapsed = time.time() - start_time
                        throughput = total_sent / elapsed if elapsed > 0 else 0
                        logger.info(f"📊 Sent {total_sent} messages | Throughput: {throughput:.2f} msg/s")
                    
                    time.sleep(delay)
                
                if not loop:
                    break
                
                logger.info("🔄 Looping: restarting from beginning...")
            
            # Flush remaining messages
            self.producer.flush()
            elapsed = time.time() - start_time
            logger.info(f"✅ Streaming complete!")
            logger.info(f"📈 Total messages: {total_sent}")
            logger.info(f"⏱️  Duration: {elapsed:.2f}s")
            logger.info(f"🚀 Avg throughput: {total_sent/elapsed:.2f} msg/s")
            
        except Exception as e:
            logger.error(f"❌ Error streaming data: {e}", exc_info=True)
        finally:
            self.producer.close()
            logger.info("🔌 Producer closed")
    
    def _on_success(self, txn_id, record_metadata):
        """Callback for successful message delivery"""
        if int(txn_id.replace('TXN', '')) % 500 == 0:  # Log occasionally
            logger.debug(
                f"✓ {txn_id} → partition={record_metadata.partition} "
                f"offset={record_metadata.offset}"
            )
    
    def _on_error(self, exc):
        """Callback for failed message delivery"""
        logger.error(f"❌ Message delivery failed: {exc}", exc_info=True)


def main():
    """Main entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Kafka Producer for Retail Transactions')
    parser.add_argument('--file', default='retail_data_bangalore.csv', help='CSV file path')
    parser.add_argument('--broker', default='localhost:9092', help='Kafka broker address')
    parser.add_argument('--topic', default='retail.transactions', help='Kafka topic')
    parser.add_argument('--delay', type=float, default=0.05, help='Delay between messages (seconds)')
    parser.add_argument('--loop', action='store_true', help='Loop continuously')
    
    args = parser.parse_args()
    
    logger.info("🚀 Starting LiveInsight+ Kafka Producer")
    logger.info("=" * 60)
    
    producer = RetailKafkaProducer(
        bootstrap_servers=args.broker,
        topic=args.topic
    )
    
    producer.send_data(
        file_path=args.file,
        delay=args.delay,
        loop=args.loop
    )


if __name__ == "__main__":
    main()
