# 📦 LiveInsight+ Installation Guide

Complete setup instructions for Windows, Linux, and macOS

---

## 🖥️ Windows Installation

### Step 1: Install Python
1. Download Python 3.9+ from https://www.python.org/downloads/
2. Run installer, **check "Add Python to PATH"**
3. Verify installation:
```powershell
python --version
pip --version
```

### Step 2: Install Java (for Kafka)
1. Download Java 11+ from https://adoptium.net/
2. Install and verify:
```powershell
java -version
```

### Step 3: Install Apache Kafka
1. Download Kafka from https://kafka.apache.org/downloads
2. Extract to `C:\kafka`
3. Test extraction:
```powershell
ls C:\kafka\bin\windows
```

### Step 4: Install Python Dependencies
```powershell
cd "C:\Users\borut\Desktop\main el"
pip install -r requirements.txt
```

**If errors occur:**
```powershell
# Update pip first
python -m pip install --upgrade pip

# Install with verbose output
pip install -r requirements.txt -v

# Install individually if batch fails
pip install kafka-python fastapi uvicorn streamlit pandas numpy scikit-learn shap plotly psutil requests
```

### Step 5: Configure Windows Firewall
```powershell
# Allow Kafka ports (optional, for network access)
New-NetFirewallRule -DisplayName "Kafka" -Direction Inbound -LocalPort 9092 -Protocol TCP -Action Allow
New-NetFirewallRule -DisplayName "Zookeeper" -Direction Inbound -LocalPort 2181 -Protocol TCP -Action Allow
```

### Step 6: Start Kafka
**Terminal 1 (Zookeeper):**
```powershell
cd C:\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

**Terminal 2 (Kafka):**
```powershell
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

**Wait for:** `[KafkaServer id=0] started`

### Step 7: Create Kafka Topic
**Terminal 3:**
```powershell
cd C:\kafka
.\bin\windows\kafka-topics.bat --create --topic retail.transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
```

**Verify topic:**
```powershell
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

### Step 8: Launch LiveInsight+
**Option A: Automated**
```powershell
cd "C:\Users\borut\Desktop\main el"
.\start_all.ps1
```

**Option B: Manual (5 terminals)**
```powershell
# Terminal 4: Producer
python stream_server_kafka.py --delay 0.05 --loop

# Terminal 5: Processor
python processor_consumer.py --checkpoint 3

# Terminal 6: ML Service
python ml_service.py

# Terminal 7: Agent
python agent.py --interval 30

# Terminal 8: Dashboard
streamlit run dashboard_new.py
```

### Step 9: Verify System
Open browser:
- Dashboard: http://localhost:8501
- ML API: http://localhost:8000
- API Docs: http://localhost:8000/docs

---

## 🐧 Linux Installation

### Step 1: Install Python
```bash
# Ubuntu/Debian
sudo apt update
sudo apt install python3.9 python3-pip python3-venv -y

# CentOS/RHEL
sudo yum install python39 python39-pip -y

# Verify
python3 --version
pip3 --version
```

### Step 2: Install Java
```bash
# Ubuntu/Debian
sudo apt install openjdk-11-jdk -y

# CentOS/RHEL
sudo yum install java-11-openjdk -y

# Verify
java -version
```

### Step 3: Install Apache Kafka
```bash
# Download and extract
cd ~
wget https://downloads.apache.org/kafka/3.6.0/kafka_2.13-3.6.0.tgz
tar -xzf kafka_2.13-3.6.0.tgz
cd kafka_2.13-3.6.0

# Add to PATH (optional)
echo 'export PATH=$PATH:~/kafka_2.13-3.6.0/bin' >> ~/.bashrc
source ~/.bashrc
```

### Step 4: Install Python Dependencies
```bash
cd ~/liveinsight-plus
pip3 install -r requirements.txt

# If permission errors
pip3 install -r requirements.txt --user
```

### Step 5: Start Kafka
**Terminal 1 (Zookeeper):**
```bash
cd ~/kafka_2.13-3.6.0
bin/zookeeper-server-start.sh config/zookeeper.properties
```

**Terminal 2 (Kafka):**
```bash
cd ~/kafka_2.13-3.6.0
bin/kafka-server-start.sh config/server.properties
```

### Step 6: Create Kafka Topic
**Terminal 3:**
```bash
cd ~/kafka_2.13-3.6.0
bin/kafka-topics.sh --create --topic retail.transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Verify
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Step 7: Launch LiveInsight+
```bash
cd ~/liveinsight-plus
chmod +x start_all.sh
./start_all.sh
```

**Or manually:**
```bash
# Terminal 4: Producer
python3 stream_server_kafka.py --delay 0.05 --loop

# Terminal 5: Processor
python3 processor_consumer.py --checkpoint 3

# Terminal 6: ML Service
python3 ml_service.py

# Terminal 7: Agent
python3 agent.py --interval 30

# Terminal 8: Dashboard
streamlit run dashboard_new.py
```

---

## 🍎 macOS Installation

### Step 1: Install Homebrew
```bash
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

### Step 2: Install Python
```bash
brew install python@3.9
python3 --version
```

### Step 3: Install Java
```bash
brew install openjdk@11
java -version
```

### Step 4: Install Apache Kafka
```bash
brew install kafka

# Verify
kafka-topics --version
```

### Step 5: Install Python Dependencies
```bash
cd ~/liveinsight-plus
pip3 install -r requirements.txt
```

### Step 6: Start Kafka (Homebrew manages services)
```bash
# Start Zookeeper
brew services start zookeeper

# Start Kafka
brew services start kafka

# Verify
brew services list
```

### Step 7: Create Kafka Topic
```bash
kafka-topics --create --topic retail.transactions --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

# Verify
kafka-topics --list --bootstrap-server localhost:9092
```

### Step 8: Launch LiveInsight+
```bash
cd ~/liveinsight-plus
chmod +x start_all.sh
./start_all.sh
```

---

## 🐳 Docker Installation (Optional)

### Using Docker Compose
```bash
# Create docker-compose.yml
cat > docker-compose.yml <<EOF
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"
  
  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
EOF

# Start Kafka
docker-compose up -d

# Verify
docker-compose ps
```

Then follow OS-specific Python setup above.

---

## 🔧 Troubleshooting

### Issue: Kafka won't start
**Windows:**
```powershell
# Check Java
java -version

# Check ports
netstat -ano | findstr :9092
netstat -ano | findstr :2181

# Kill processes if needed
taskkill /F /PID <PID>
```

**Linux/Mac:**
```bash
# Check Java
java -version

# Check ports
lsof -i :9092
lsof -i :2181

# Kill processes
kill -9 <PID>
```

### Issue: Python package conflicts
```powershell
# Create virtual environment
python -m venv venv

# Activate (Windows)
.\venv\Scripts\activate

# Activate (Linux/Mac)
source venv/bin/activate

# Install in venv
pip install -r requirements.txt
```

### Issue: SHAP installation fails
```powershell
# Install build tools (Windows)
pip install --upgrade setuptools wheel

# Or use pre-built wheel
pip install shap --only-binary :all:
```

### Issue: Streamlit won't start
```powershell
# Check port
netstat -ano | findstr :8501

# Kill process
taskkill /F /PID <PID>

# Or use different port
streamlit run dashboard_new.py --server.port 8502
```

### Issue: ML service training fails
```powershell
# Ensure processor generated data
ls output/product_inventory_usage.csv

# Wait for more data (30 seconds)
timeout 30

# Manually trigger
curl -X POST http://localhost:8000/train
```

---

## 📋 Verification Checklist

After installation, verify:

- ✅ Python 3.9+ installed
- ✅ Java 11+ installed
- ✅ Kafka downloaded and extracted
- ✅ Zookeeper running
- ✅ Kafka broker running
- ✅ Topic `retail.transactions` created
- ✅ Python dependencies installed
- ✅ Producer sending messages
- ✅ Processor writing CSV files
- ✅ ML service responding (http://localhost:8000)
- ✅ Agent creating actions
- ✅ Dashboard accessible (http://localhost:8501)

---

## 🚀 Next Steps

1. Read `RUNBOOK.md` for detailed operations guide
2. Review `PROJECT_SUMMARY.md` for architecture overview
3. Check `QUICK_REFERENCE.md` for commands
4. Open Dashboard at http://localhost:8501
5. Explore ML API at http://localhost:8000/docs

---

## 📞 Support

If installation issues persist:

1. Check logs in each terminal
2. Review `config.ini` for settings
3. Verify all ports are available
4. Ensure firewall allows connections
5. Try Docker installation as alternative

---

**Installation complete! Ready to run LiveInsight+ 🚀**
