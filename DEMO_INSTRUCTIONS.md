# VexDB Adaptive Migration Demo Instructions

## 🚀 How to Run the Migration Demo

### Option 1: Quick Start (Recommended)

1. **Start the Server:**
   ```bash
   # Double-click the batch file OR run in terminal:
   start_server_demo.bat
   ```
   
   Wait for the server to show:
   ```
   INFO: Uvicorn running on http://0.0.0.0:8000
   INFO: Application startup complete.
   ```

2. **Run the Demo (in a new terminal):**
   ```bash
   venv\Scripts\python.exe scripts/simple_migration_demo.py
   ```

### Option 2: Manual Control

1. **Start Server Manually:**
   ```bash
   venv\Scripts\python.exe -m uvicorn vexdb.main:app --reload --host 0.0.0.0 --port 8000
   ```

2. **Test Basic Functionality:**
   ```bash
   venv\Scripts\python.exe scripts/test_server.py
   ```

3. **Run Full Demo:**
   ```bash
   venv\Scripts\python.exe scripts/simple_migration_demo.py
   ```

## 📊 What the Demo Does

The demo will automatically:

### Phase 1: Initial State
- Check server health
- Show starting system status (should be `brute_force` algorithm)

### Phase 2: Load Small Dataset
- Add 50 clustered vectors (128 dimensions)
- Test initial query performance

### Phase 3: Trigger First Optimization
- Add 200 more vectors (total ~250)
- Manually trigger optimization
- **Watch for algorithm migration!** (might migrate to IVF)

### Phase 4: Add Diverse Data
- Add 100 sparse, high-dimensional vectors (256D, 70% sparse)
- **This should trigger LSH migration!**

### Phase 5: Final Optimization
- Trigger final optimization
- Show performance evolution

## 🔍 What to Watch For

### In the Server Logs:
- `🔧 Algorithm selection logic`
- `🚀 Migration processes`
- `📊 Performance metrics`
- `⚠️ Optimization decisions`

### In the Demo Logs:
- Algorithm changes: `brute_force → ivf → lsh`
- Query time improvements
- System status evolution

## 📁 Generated Files

- **Server Logs**: Look at the server terminal output
- **Demo Logs**: `logs/simple_demo_YYYYMMDD_HHMMSS.log`
- **Detailed Logs**: `logs/vexdb_migration_demo_*.log` (if running full demo)

## 🎯 Expected Behavior

1. **Start**: `brute_force` (default for small datasets)
2. **After 250+ clustered vectors**: Likely migrates to `ivf` (good for clustered data)
3. **After sparse high-dim vectors**: Might migrate to `lsh` (optimal for sparse data)

## 🛠️ Troubleshooting

### Server Won't Start:
```bash
# Check if virtual environment is working:
venv\Scripts\python.exe --version

# Check if VexDB imports:
venv\Scripts\python.exe -c "import vexdb; print('OK')"
```

### Demo Connection Errors:
- Make sure server is running on port 8000
- Check firewall settings
- Try `http://localhost:8000/health` in browser

### Missing Dependencies:
```bash
# Install missing packages in venv:
venv\Scripts\python.exe -m pip install requests numpy
```

## 🎉 Success Indicators

✅ **Working Migration System:**
- Server starts without errors
- Algorithms change during demo: `brute_force → ivf/lsh`
- Query performance adapts to data characteristics
- No error messages in logs

✅ **Expected Log Messages:**
- "Migration initiated"
- "Algorithm selection: [algorithm] with confidence [X]"
- "Index manager status updated"
- "Query routing: using [algorithm]"

---

**🚀 Ready to see VexDB's adaptive intelligence in action!**

The system will automatically analyze your data, select optimal algorithms, and migrate seamlessly between them - all while maintaining query availability!
