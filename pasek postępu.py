from time import sleep
from tqdm import tqdm

for i in tqdm(range(100), desc="Przetwarzanie", unit="krok"):
    sleep(0.05)  # krok postępu 


