FROM python:alpine3.12
WORKDIR /app
ADD ./requirements.txt .
RUN python -m pip install -r requirements.txt
ADD ./chordlite ./chordlite
ADD ./tests ./tests
RUN python -m pytest tests
# ADD ./.pylintrc .
# RUN python -m pylint chordlite
ADD ./dht_service ./dht_service
ENTRYPOINT [ "python", "-m", "dht_service" ]
