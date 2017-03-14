FROM python:3-onbuild
RUN useradd -ms /bin/bash admin
USER admin
CMD [ "python", "./start.py" ]