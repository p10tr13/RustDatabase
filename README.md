# Opis struktury projektu
1. `Cargo.toml`
   - Konfiguracja projektu i zależności (pest, clap, thiserror).
2. `main.rs`
   - Punkt wejścia aplikacji. Odpowiada za pętlę REPL (konsolę), obsługę argumentów CLI, oraz operacje IO (`SAVE_AS`/`READ_FROM`).
3. `lib.rs`
   - Główny plik biblioteki eksportuje publiczne moduły.
4. `database.rs`
   - Moduł odpowiedzialny za przechowywanie danych. Definiuje struktury `Table` i `Database`. Zawiera enum `AnyDatabase` oraz mechanizm dispatchu generycznego, który pozwala obsługiwać bazy o kluczach `String` lub `i64`.
5. `command.rs`
   - Implementacja komend wykonywanych na bazie danych. Każde polecenie SQL ma tu swoją strukturę implementującą trait `Command`. Odpowiada za logikę biznesową i walidację.
6. `queries.rs`
   - Warstwa tłumacząca tekst na zapytania. Wykorzystuje bibliotekę Pest do parsowania wejścia i mapuje je na enum `Query`.
7. `grammar.pest`
   - Plik definicji gramatyki. Określa składnię poleceń SQL akceptowaną przez bazę.
8. `domain.rs`
   - Definicje typów danych. Zawiera enum `Value` (obsługujący `Int`, `Float`, `Bool`, `String`), `DataType` oraz strukturę `Record`.
9. `error.rs`
   - Obsługa błędów definiująca enum `DbError` przy użyciu biblioteki `thiserror`, który unifikuje błędy IO, parsowania oraz logiki bazy danych.

# Mój ulubiony moduł
Na pewno jest nim `queries.rs`. Był on bardzo satysfakcjonujący do pisania ze względu na wykorzystanie wiedzy z przedmiotu
"Metody Translacji". Nie spodziewałem się, że wiedza zdobyta na tym przedmiocie przyda mi się tak szybko oraz sama
gramatyka, którą mieliśmy napisać, nie miała wielu "edge case-ów", które by znacząco utrudniały zadanie.

# Zasady komend
Jednym z ograniczeń tego programu jest to, że komendy powinny być w jednej linii. Jest taki wymóg ze względu na brak znaku ";"
w gramatyce, który by jasno pokazywał, gdzie się zapytanie kończy.

# Obsługa programu
Program wywołujemy przy pomocy komendy: `cargo run -- --key-type string` lub `cargo run -- -k string`, gdzie string może
być zamieniony na `int`. Program kończy działanie, gdy wczyta komendę `quit` lub `exit`.

# Uwaga
Należy zwracać uwagę na to, aby pliki wczytywane z komendy `READ_FROM` nie miały pętli. `READ_FROM` od razu wywoła
wczytane komendy, może to doprowadzić do nieskończonych rekurencji i potencjalnego błędu programu. Jednakże to pozwala 
na zagnieżdżanie się skryptów i modułowe budowanie bazy danych z wielu plików.