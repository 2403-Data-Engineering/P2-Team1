# P2-Team1
#### Diego Perez-Torres, Jonathan Urena-Salazar, John Magri, Juan Aracena, Akanksha Satya

## Environment setup (Windows instructions)
Need to make sure you're using Python 3.11 venv 
```bash
py -3.11 -m venv .venv
```

Confirm using correct version (should display Python 3.14.0)
```bash
python --version
```
Spark requires Java. Specifically:

    Java 8, 11, or 17 (Java 17 is recommended for Spark 3.4+)
    The JAVA_HOME environment variable must be set
    
Verify Java is installed:
```bash
java -version
```

If you see a version number (1.8.x, 11.x, or 17.x), you're good.
Install all dependencies 
```python
pip install -r requirements.txt
```
