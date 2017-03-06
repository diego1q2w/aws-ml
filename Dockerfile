FROM python:3-onbuild
RUN mkdir /data
CMD [ "python", "./start.py" ]