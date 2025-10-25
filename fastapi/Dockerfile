FROM continuumio/miniconda3

RUN apt-get update -y 
RUN apt-get install nano unzip
RUN apt-get install -y python3.10
RUN apt install curl -y

RUN useradd -m -u 1000 user
USER user

ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH

WORKDIR $HOME/app

RUN curl -fsSL https://get.deta.dev/cli.sh | sh

COPY requirements.txt /dependencies/requirements.txt
RUN pip install -r /dependencies/requirements.txt

COPY --chown=user . $HOME/app

CMD fastapi run ./src/app.py --port $PORT