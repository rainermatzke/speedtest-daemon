Docker Image erzeugen:
    docker build -t speedtest .

Docker Image starten:
(Pfad auf Zielvereichnis anpassen)
    docker run -v <Zielverzeichnis>:/samples speedtest
    Bsp.:
    docker run -v //c/Users/raine:/samples speedtest