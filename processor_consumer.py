"""
Kafka Stream Processor for LiveInsight+
Stateful aggregation engine with windowing - replaces Spark DStreams
Implements running totals, time-based windows, and snapshot exports
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, Any
import pandas as pd
from kafka import KafkaConsumer
from kafka.errors import KafkaError

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StatefulProcessor:
    """
    Stateful stream processor with windowing and checkpointing
    Demonstrates real-time aggregation without Spark/MapReduce
    """
    
    def __init__(self, bootstrap_servers='localhost:9092', 
                 topic='retail.transactions',
                 checkpoint_interval=5.0):
        """
        Initialize processor with Kafka consumer
        
        Args:
            bootstrap_servers: Kafka broker address
            topic: Topic to consume from
            checkpoint_interval: Seconds between snapshot writes
        """
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            auto_commit_interval_ms=1000,
            group_id='liveinsight-processor-v1',
            max_poll_records=500,
            session_timeout_ms=30000
        )
        
        self.checkpoint_interval = checkpoint_interval
        self.last_checkpoint = time.time()
        
        # State stores (in-memory)
        self.state = {
            'branch_sales': defaultdict(float),
            'category_sales': defaultdict(float),
            'product_sales': defaultdict(int),
            'product_revenue': defaultdict(float),
            'payment_counts': defaultdict(int),
            'branch_time_demand': defaultdict(float),
            'weekly_branch_revenue': defaultdict(float),
            'monthly_branch_revenue': defaultdict(float),
            'product_inventory_usage': defaultdict(int),
            'hourly_transactions': defaultdict(int),
            'customer_transactions': defaultdict(int)
        }
        
        # Performance metrics
        self.metrics = {
            'messages_processed': 0,
            'start_time': time.time(),
            'last_throughput_check': time.time(),
            'messages_since_last_check': 0
        }
        
        # Output directory
        os.makedirs('output', exist_ok=True)
        
        logger.info("✅ Stream processor initialized")
        logger.info(f"📡 Consuming from: {topic}")
        logger.info(f"💾 Checkpoint interval: {checkpoint_interval}s")
    
    def parse_transaction(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Parse and enrich transaction message
        
        Args:
            msg: Raw Kafka message
            
        Returns:
            Enriched transaction dict
        """
        try:
            ts = datetime.strptime(msg['Timestamp'], "%Y-%m-%d %H:%M:%S")
            hour = ts.hour
            week = ts.strftime('%Y-W%U')
            month = ts.strftime('%Y-%m')
            
            # Time slot classification
            if 6 <= hour < 12:
                time_slot = "Morning"
            elif 12 <= hour < 17:
                time_slot = "Afternoon"
            elif 17 <= hour < 21:
                time_slot = "Evening"
            else:
                time_slot = "Night"
            
            return {
                'TransactionID': msg['TransactionID'],
                'Timestamp': ts,
                'BranchName': msg['BranchName'],
                'Category': msg['Category'],
                'Product': msg['Product'],
                'Quantity': msg['Quantity'],
                'FinalAmount': msg['FinalAmount'],
                'PaymentType': msg['PaymentType'],
                'CustomerID': msg['CustomerID'],
                'TimeSlot': time_slot,
                'Week': week,
                'Month': month,
                'Hour': hour
            }
        except Exception as e:
            logger.error(f"Parse error: {e} | Message: {msg}")
            return None
    
    def update_state(self, txn: Dict[str, Any]):
        """
        Update all aggregation state stores
        
        Args:
            txn: Parsed transaction
        """
        # Branch sales
        self.state['branch_sales'][txn['BranchName']] += txn['FinalAmount']
        
        # Category sales
        self.state['category_sales'][txn['Category']] += txn['FinalAmount']
        
        # Product sales (units)
        self.state['product_sales'][txn['Product']] += txn['Quantity']
        
        # Product revenue
        self.state['product_revenue'][txn['Product']] += txn['FinalAmount']
        
        # Payment type counts
        self.state['payment_counts'][txn['PaymentType']] += 1
        
        # Branch-TimeSlot demand
        key = (txn['BranchName'], txn['TimeSlot'])
        self.state['branch_time_demand'][key] += txn['FinalAmount']
        
        # Weekly branch revenue
        key = (txn['BranchName'], txn['Week'])
        self.state['weekly_branch_revenue'][key] += txn['FinalAmount']
        
        # Monthly branch revenue
        key = (txn['BranchName'], txn['Month'])
        self.state['monthly_branch_revenue'][key] += txn['FinalAmount']
        
        # Product inventory usage (total units sold)
        self.state['product_inventory_usage'][txn['Product']] += txn['Quantity']
        
        # Hourly transaction counts
        hour_key = txn['Timestamp'].strftime('%Y-%m-%d %H:00')
        self.state['hourly_transactions'][hour_key] += 1
        
        # Customer transaction counts
        self.state['customer_transactions'][txn['CustomerID']] += 1
    
    def checkpoint_state(self):
        """Write state snapshots to CSV files"""
        try:
            # Branch sales
            df = pd.DataFrame([
                {'BranchName': k, 'TotalRevenue': v}
                for k, v in self.state['branch_sales'].items()
            ])
            df.to_csv('output/branch_sales.csv', index=False)
            
            # Category sales
            df = pd.DataFrame([
                {'Category': k, 'TotalRevenue': v}
                for k, v in self.state['category_sales'].items()
            ])
            df.to_csv('output/category_sales.csv', index=False)
            
            # Product sales (units)
            df = pd.DataFrame([
                {'Product': k, 'UnitsSold': v}
                for k, v in self.state['product_sales'].items()
            ])
            df.to_csv('output/product_sales.csv', index=False)
            
            # Payment type analysis
            df = pd.DataFrame([
                {'PaymentType': k, 'TransactionCount': v}
                for k, v in self.state['payment_counts'].items()
            ])
            df.to_csv('output/payment_type_analysis.csv', index=False)
            
            # Branch-time demand
            df = pd.DataFrame([
                {'BranchName': k[0], 'TimeSlot': k[1], 'TotalRevenue': v}
                for k, v in self.state['branch_time_demand'].items()
            ])
            df.to_csv('output/branch_time_demand.csv', index=False)
            
            # Weekly branch revenue
            df = pd.DataFrame([
                {'BranchName': k[0], 'Week': k[1], 'TotalRevenue': v}
                for k, v in self.state['weekly_branch_revenue'].items()
            ])
            df.to_csv('output/weekly_branch_revenue.csv', index=False)
            
            # Monthly branch revenue
            df = pd.DataFrame([
                {'BranchName': k[0], 'Month': k[1], 'TotalRevenue': v}
                for k, v in self.state['monthly_branch_revenue'].items()
            ])
            df.to_csv('output/monthly_branch_revenue.csv', index=False)
            
            # Product inventory usage (for ML service)
            df = pd.DataFrame([
                {'Product': k, 'TotalUnitsSold': v}
                for k, v in self.state['product_inventory_usage'].items()
            ])
            df.to_csv('output/product_inventory_usage.csv', index=False)
            
            # Hourly transactions
            df = pd.DataFrame([
                {'Hour': k, 'TransactionCount': v}
                for k, v in self.state['hourly_transactions'].items()
            ])
            df.to_csv('output/hourly_transactions.csv', index=False)
            
            logger.info("💾 Checkpoint saved (9 aggregates)")
            
        except Exception as e:
            logger.error(f"Checkpoint error: {e}", exc_info=True)
    
    def log_metrics(self):
        """Log throughput and latency metrics"""
        now = time.time()
        elapsed = now - self.metrics['start_time']
        total_msgs = self.metrics['messages_processed']
        
        # Calculate throughput since last check
        time_since_check = now - self.metrics['last_throughput_check']
        if time_since_check >= 10.0:  # Log every 10s
            recent_msgs = self.metrics['messages_since_last_check']
            throughput = recent_msgs / time_since_check
            
            logger.info("=" * 70)
            logger.info(f"📊 METRICS | Total: {total_msgs} msgs | "
                       f"Uptime: {elapsed:.1f}s | "
                       f"Throughput: {throughput:.2f} msg/s")
            logger.info("=" * 70)
            
            self.metrics['last_throughput_check'] = now
            self.metrics['messages_since_last_check'] = 0
    
    def run(self):
        """Main processing loop"""
        logger.info("🚀 Stream processor started")
        logger.info("⏳ Waiting for messages...")
        
        try:
            for message in self.consumer:
                # Parse transaction
                txn = self.parse_transaction(message.value)
                if txn is None:
                    continue
                
                # Update state
                self.update_state(txn)
                
                # Update metrics
                self.metrics['messages_processed'] += 1
                self.metrics['messages_since_last_check'] += 1
                
                # Checkpoint if needed
                now = time.time()
                if now - self.last_checkpoint >= self.checkpoint_interval:
                    self.checkpoint_state()
                    self.last_checkpoint = now
                
                # Log metrics periodically
                self.log_metrics()
                
        except KeyboardInterrupt:
            logger.info("⚠️  Received shutdown signal")
        except Exception as e:
            logger.error(f"❌ Processing error: {e}", exc_info=True)
        finally:
            # Final checkpoint
            logger.info("💾 Writing final checkpoint...")
            self.checkpoint_state()
            
            # Close consumer
            self.consumer.close()
            
            # Final metrics
            elapsed = time.time() - self.metrics['start_time']
            total = self.metrics['messages_processed']
            avg_throughput = total / elapsed if elapsed > 0 else 0
            
            logger.info("=" * 70)
            logger.info("📈 FINAL STATS")
            logger.info(f"   Total messages: {total}")
            logger.info(f"   Total time: {elapsed:.2f}s")
            logger.info(f"   Avg throughput: {avg_throughput:.2f} msg/s")
            logger.info("=" * 70)
            logger.info("✅ Processor shutdown complete")


def main():
    """Entry point"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Kafka Stream Processor')
    parser.add_argument('--broker', default='localhost:9092', help='Kafka broker')
    parser.add_argument('--topic', default='retail.transactions', help='Topic to consume')
    parser.add_argument('--checkpoint', type=float, default=3.0, help='Checkpoint interval (seconds)')
    
    args = parser.parse_args()
    
    processor = StatefulProcessor(
        bootstrap_servers=args.broker,
        topic=args.topic,
        checkpoint_interval=args.checkpoint
    )
    
    processor.run()


if __name__ == "__main__":
    main()
