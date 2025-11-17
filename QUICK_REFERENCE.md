# 🚀 LiveInsight+ Quick Reference

## Start System (Windows)
```powershell
.\start_all.ps1
```

## Manual Start (5 Terminals)
```powershell
# 1. Producer
python stream_server_kafka.py --delay 0.05 --loop

# 2. Processor  
python processor_consumer.py --checkpoint 3

# 3. ML Service
python ml_service.py

# 4. Agent
python agent.py --interval 30

# 5. Dashboard
streamlit run dashboard_new.py
```

## URLs
- 📊 Dashboard: http://localhost:8501
- 🔬 ML API: http://localhost:8000
- 📚 API Docs: http://localhost:8000/docs

## Quick Test Commands
```powershell
# Check ML service
curl http://localhost:8000/

# Trigger batch prediction
curl -X POST http://localhost:8000/batch-predict

# Get SHAP explanation
curl -X POST http://localhost:8000/explain -H "Content-Type: application/json" -d "{\"product\": \"Chocolate\"}"

# Run agent once
python agent.py --once
```

## Stop All Services
```powershell
# Windows
Get-Process python,streamlit | Stop-Process

# Linux/Mac
pkill -f 'python3.*kafka|processor|ml_service|agent'
pkill -f streamlit
```

## Output Files
```
output/
├── branch_sales.csv              # Branch revenue aggregates
├── category_sales.csv            # Category revenue aggregates
├── product_sales.csv             # Product units sold
├── payment_type_analysis.csv     # Payment method counts
├── product_inventory_usage.csv   # Inventory consumption
├── predictions.csv               # ML predictions
├── agent_actions.csv             # Agent decisions
└── xai/*.json                    # SHAP explanations
```

## Key Metrics
- 📈 Producer: 1500-2000 msg/s
- ⚡ Latency: 20-30ms
- 🧠 ML: 50-80ms prediction
- 🔍 SHAP: 2-4s computation
- 🔄 Agent: 30s decision cycle

## Dashboard Tabs
1. **Real-Time Analytics** - KPIs, branch/product charts
2. **ML Predictions & XAI** - SHAP waterfall, feature importance
3. **Agent Actions** - Pending approvals, urgency levels
4. **Inventory Deep Dive** - Heatmap, distributions
5. **Stream Performance** - System resources, throughput

## Troubleshooting
```powershell
# Check Kafka
Get-Process | Where-Object {$_.ProcessName -like "*kafka*"}

# Check data exists
ls output/product_inventory_usage.csv

# Restart producer
Get-Process -Name python | Where-Object {$_.CommandLine -like "*stream_server*"} | Stop-Process
python stream_server_kafka.py --delay 0.05 --loop
```

## Demo Script (5 min)
1. [0:30] Show all services running
2. [1:00] Tab 1: Real-time analytics
3. [1:00] Tab 2: ML + SHAP explanation
4. [1:00] Tab 3: Agent actions + approve
5. [1:00] Tab 4: Inventory heatmap
6. [0:30] Closing: production-ready architecture

## Talking Points
- ✅ Kafka streaming (no Spark)
- ✅ SHAP for explainability
- ✅ Agent uses XAI evidence
- ✅ Human-in-loop safety
- ✅ 1500+ msg/s throughput
- ✅ Production-ready APIs

## Emergency Commands
```powershell
# Restart everything
Get-Process python,streamlit | Stop-Process
Start-Sleep 2
.\start_all.ps1

# Clear output
Remove-Item -Recurse -Force output/*
New-Item -ItemType Directory -Path output

# Reinstall deps
pip install -r requirements.txt --force-reinstall
```

## File Sizes
- stream_server_kafka.py: 160 lines
- processor_consumer.py: 330 lines
- ml_service.py: 480 lines
- agent.py: 340 lines
- dashboard_new.py: 680 lines
- **Total: ~2000 lines**

## Course Coverage
- **AI372IA** Stream Processing ✅
- **AI374TFB** Explainable AI ✅
- **AI373IA** Agentic AI ✅

---

**Good luck! 🎓🚀**
