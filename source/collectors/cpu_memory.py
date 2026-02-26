import psutil

def collect_cpu_memory():
    cpu = psutil.cpu_percent(interval=None)
    memory = psutil.virtual_memory().percent
    return cpu, memory