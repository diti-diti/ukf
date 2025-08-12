RBN – TOP CW heard in Poland (Streamlit)

Opis

Aplikacja pobiera dzienne archiwa Reverse Beacon Network (RBN) z okresu wskazanego przez użytkownika, wyciąga z nich wpisy CW odebrane przez polskie skimmery (prefix kraju odbiorcy = SP) i tworzy ranking TOP-N najczęściej słyszanych znaków (dx).
Interfejs działa w przeglądarce dzięki Streamlit – pozwala wybrać zakres dat, pasmo i rozmiar rankingu oraz pobrać wyniki jako CSV i TXT (format dla MorseRunnera).

⸻

Wymagania
	•	Python 3.9+
	•	Pakiety: streamlit, pandas

pip install streamlit pandas



⸻

Uruchomienie
	1.	Zapisz plik jako app.py.
	2.	(Opcjonalnie) dodaj do PATH: ~/Library/Python/3.9/bin – na macOS Streamlit bywa instalowany tam.
	3.	Start:

streamlit run app.py

albo pełną ścieżką:

~/Library/Python/3.9/bin/streamlit run app.py


	4.	Aplikacja otworzy się pod adresem http://localhost:8501.

⸻

Jak to działa (skrót architektury)

Źródło danych
	•	Pliki ZIP dzienne:
https://data.reversebeacon.net/rbn_history/YYYYMMDD.zip
	•	Wewnątrz ZIPa – CSV bez nagłówka (13 lub 15 kolumn) albo CSV z nagłówkiem w formacie „telegraphy”.

Główne kroki
	1.	UI (Streamlit): wybór zakresu dat, pasma, TOP-N, opcja pobierania brakujących ZIP-ów.
	2.	Pobieranie: download_zip() – zapisuje ZIP do ./data/ (z prostym retry; omija, gdy plik już jest).
	3.	Wczytywanie pliku: read_one_csv_from_zip()
	•	automatyczna detekcja formatu CSV:
	•	15 kolumn (raw),
	•	13 kolumn (raw),
	•	13 kolumn z nagłówkiem: callsign,de_pfx,...,tx_mode
	•	zwraca minimalny DataFrame: kolumny src_pfx (prefix kraju skimmera), dx (nadawca), mode, band.
	4.	Agregacja: aggregate_counts()
	•	filtruje: src_pfx == "SP" i mode == "CW", opcjonalnie band.
	•	zlicza częstotliwość wystąpień dx (Counter → TOP-N).
	•	wynik cache’owany przez @st.cache_data.
	5.	Wynik:
	•	tabela i wykres słupkowy (Streamlit),
	•	przyciski Pobierz CSV (top_calls_sp_cw.csv) i Pobierz TXT (morse_runner_calls.txt).

⸻

Opis funkcji

daterange(d1, d2)

Generator kolejnych dni w zakresie [d1, d2].

url_for(d)

Buduje URL do dziennego archiwum RBN dla daty d.

ensure_dir(path)

Tworzy katalog, jeśli nie istnieje.

download_zip(dst, url, retries=2, timeout=50)

Pobiera ZIP z RBN (z ponawianiem). Zwraca True/False. Pomija pobieranie, jeśli plik już istnieje i ma rozmiar > 0.

read_one_csv_from_zip(zpath)

Otwiera ZIP, wykrywa wariant CSV i zwraca DataFrame z kolumnami:
src_pfx (prefix kraju skimmera/odbiorcy), dx (nadawca), mode, band.
W razie błędu/nieobsługiwanego formatu zwraca None.

aggregate_counts(date_from, date_to, band_filter, topn, fetch=True)

Pętla po dniach: (opcjonalnie) pobiera ZIP, wczytuje CSV, filtruje heard in SP + CW, opcjonalnie po paśmie, zlicza dx i zwraca DataFrame TOP-N (callsign, count).
Zastosowany @st.cache_data – wyniki dla tego samego zestawu parametrów są cache’owane.

⸻

Interfejs (UI)
	•	From / To – zakres dat (UTC).
	•	TOP N – rozmiar rankingu (50–2000).
	•	Band – filtr pasma: all lub jedno z 160m…10m.
	•	Fetch missing ZIPs – zaznacz, by aplikacja automatycznie pobierała brakujące archiwa do ./data/.
	•	CALCULATE – uruchamia obliczenia. Pokazany jest pasek postępu.
	•	Po obliczeniu:
	•	Tabela wynikowa,
	•	Wykres słupkowy,
	•	Download CSV i Download TXT (MorseRunner).

⸻

Format wyjścia
	•	top_calls_sp_cw.csv:

callsign,count
SP9XYZ,1234
DL1ABC,987
...


	•	morse_runner_calls.txt:
Jeden znak na linię (bez liczników) – gotowe do MorseRunnera.

⸻

Dostosowanie / rozszerzenia
	•	Filtr po SNR lub szybkości (WPM):
w read_one_csv_from_zip() dodaj snr_db/wpm do usecols, a w aggregate_counts() dopisz filtr, np. sub = sub[sub["snr_db"].astype(int) >= 10].
	•	Wykluczenie dni contestowych:
dodaj listę dat do pominięcia przed pętlą po daterange().
	•	Ranking per pasmo:
zamiast jednego Counter użyj słownika Counter per band i wyświetl dodatkowe zakładki.
	•	Eksport do SQL/Parquet:
po agregacji zapisz df.to_parquet(...) lub to_sql(...) dla szybszych kolejnych analiz.

⸻

Rozwiązywanie problemów
	•	„command not found: streamlit” – dodaj do PATH katalog instalacji pakietu użytkownika, np.:

export PATH="$HOME/Library/Python/3.9/bin:$PATH"

lub uruchamiaj pełną ścieżką ~/Library/Python/3.9/bin/streamlit.

	•	Puste wyniki:
	•	brak ZIP-ów w ./data/ i odznaczona opcja „Fetch missing ZIPs”,
	•	zbyt wąski zakres dat lub mała aktywność polskich skimmerów w wybranym okresie,
	•	wszystkie pliki w „innym” formacie – funkcja read_one_csv_from_zip() obsługuje trzy najczęstsze, ale jeśli trafisz na kolejny wariant, dopisz mapowanie nazw kolumn.
	•	Wydajność:
szeroki zakres dat = dużo ZIP-ów → więcej czasu. Pozostaw „Fetch…” włączone i licz na tym, co już pobrane – cache znacznie przyspiesza kolejne uruchomienia.

⸻

Licencja / Uwagi
	•	Dane RBN: zgodnie z polityką Reverse Beacon Network.
	•	Kod aplikacji – wykorzystuj swobodnie do własnych analiz; pamiętaj o zachowaniu fair-use przy pobieraniu dużych zakresów danych.

Jeśli chcesz, mogę dorzucić do dokumentacji schemat kolumn dla każdego obsługiwanego formatu CSV i przykładowe rekordy.