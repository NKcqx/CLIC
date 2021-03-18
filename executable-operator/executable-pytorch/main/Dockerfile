FROM pytorch/pytorch:latest
# RUN mkdir /main
# WORKDIR /main
COPY ./requirements.txt /
RUN python -m pip install --upgrade pip && pip install -U setuptools && pip install -r /requirements.txt
# CMD ["python", "Main.py"]