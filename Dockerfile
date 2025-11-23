FROM python:3.11-slim

# Install Chrome and ChromeDriver
RUN apt-get update && apt-get install -y wget gnupg unzip
RUN wget -q -O - https://dl.google.com/linux/linux_signing_key.pub > /usr/share/keyrings/google-chrome-keyring.gpg
RUN echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome-keyring.gpg] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google.list
RUN apt-get update && apt-get install -y google-chrome-stable

# Install ChromeDriver that matches Chrome version (142.0.7444.175)
RUN wget -q -O /tmp/chromedriver.zip https://storage.googleapis.com/chrome-for-testing-public/142.0.7444.175/linux64/chromedriver-linux64.zip
RUN unzip /tmp/chromedriver.zip -d /tmp/
RUN mv /tmp/chromedriver-linux64/chromedriver /usr/local/bin/
RUN chmod +x /usr/local/bin/chromedriver

WORKDIR /app
COPY . .
RUN pip install -r requirements.txt

CMD ["python", "linkedin_scraper.py"]
