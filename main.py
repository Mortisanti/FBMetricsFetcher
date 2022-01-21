#TODO Change metrics_files to archived_metrics and download metrics file to root before archiving/removing as usual

import requests
import re
import os
from zipfile import ZipFile
from pathlib import Path
from datetime import date, datetime
from cryptography.fernet import Fernet
from ftplib import FTP
from auto_email import AutoEmailer
# These variables have awful names for the sake of obfuscation.
from my_secrets import K, F_H, F_U, F_P, FB_U, E, C_USER, XS


def main():
    fernet = Fernet(K)

    # Paths, FTP, etc.
    Path(f'metrics_files').mkdir(exist_ok=True)
    local_metrics_dir = Path(f'metrics_files').absolute()
    remote_metrics_dir = '/Metrics'
    ftp = FTP(host=fernet.decrypt(F_H).decode())
    today = str(date.today())
    log_file = 'logs.txt'
    filename_regex = r'([0-9]+_[0-9]+_[0-9]+_n\.csv)'

    # AutoEmailer variables
    email_address = '' # Email address to send from (used my gmail with the app password)
    emailer = AutoEmailer(email_address=email_address, email_passwd=fernet.decrypt(E).decode())
    msg_from = 'FBMetricsFetcher'
    msg_to = '' # Email address to send to (used my personal work address)

    cookies = {
        'wd': '1920x955',
        'locale': 'en_US',
        'c_user': fernet.decrypt(C_USER).decode(),
        'xs': fernet.decrypt(XS).decode()
    }
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
        'TE': 'trailers',
    }
    url = fernet.decrypt(FB_U).decode()

    def send_success_email():
        msg_subject = "Process Succeeded"
        msg_body = "The FB Metrics Fetcher process finished successfully."
        emailer.send_email(msg_from, msg_to, msg_subject, msg_body)

    def send_failure_email():
        msg_subject = "Process Failed"
        msg_body = "The FB Metrics Fetcher has failed to grab and drop the latest metrics file. Check the log file for errors."
        #TODO Send logs.txt attachment
        emailer.send_email(msg_from, msg_to, msg_subject, msg_body)

    def get_date_time_prefix():
        return datetime.now().strftime('%m-%d-%y %H:%M:%S') + " |"

    def get_file_timestamp(filename):
        remote_filename = filename
        timestamp = ftp.voidcmd(f'MDTM {remote_metrics_dir}/{remote_filename}')[4:].strip()
        parsed_timestamp = datetime.strptime(timestamp, '%Y%m%d%H%M%S')
        file_date = parsed_timestamp.strftime('%Y-%m-%d')
        return file_date

    def archive_metrics(filename):
        if Path(f'{local_metrics_dir}/facebook_metrics.zip').exists():
            with ZipFile(f'{local_metrics_dir}/facebook_metrics.zip', 'a') as my_zip:
                if filename in my_zip.namelist():
                    with open(log_file, 'a') as f:
                        f.write(f"{get_date_time_prefix()} Metrics file already exists in facebook_metrics.zip archive.\n")
                else:
                    my_zip.write(f'{local_metrics_dir}/{filename}', arcname=filename)
                    with open(log_file, 'a') as f:
                        f.write(f"{get_date_time_prefix()} Metrics file added to facebook_metrics.zip archive.\n")
        else:
            with ZipFile(f'{local_metrics_dir}/facebook_metrics.zip', 'x') as my_zip:
                my_zip.write(f'{local_metrics_dir}/{filename}', arcname=filename)
            with open(log_file, 'a') as f:
                f.write(f"{get_date_time_prefix()} Created facebook_metrics.zip and added metrics file to archive.\n")

    def drop_metrics(filename, file_path):
        with open(log_file, 'a') as f:
            f.write(f"{get_date_time_prefix()} Transferring metrics file...\n")
        with open(file_path, 'rb') as fp:
            ftp.storbinary(f'STOR {filename}', fp)
        with open(log_file, 'a') as f:
            f.write(f"{get_date_time_prefix()} Metrics file transferred - closing connection.\n")

    def check_and_transfer(filename, file_path):
        metrics_filename = filename
        metrics_local_path = file_path
        with open(log_file, 'a') as f:
            f.write(f"{get_date_time_prefix()} Logging into FTP...\n")
        ftp.login(user=fernet.decrypt(F_U).decode(), passwd=fernet.decrypt(F_P).decode())
        ftp.cwd(remote_metrics_dir)
        cwd_list = ftp.nlst()
        with open(log_file, 'a') as f:
            f.write(f"{get_date_time_prefix()} Logged in - checking for metrics file...\n")
        for i in cwd_list:
            if re.fullmatch(filename_regex, i):
                remote_filename = i
                if remote_filename == metrics_filename:
                    with open(log_file, 'a') as f:
                        f.write(f"{get_date_time_prefix()} Latest metrics file already transferred - closing connection...\n")
                        break
                else:
                    if today != get_file_timestamp(remote_filename):
                        with open(log_file, 'a') as f:
                            f.write(f"{get_date_time_prefix()} Outdated metrics file found - replacing...\n")
                            ftp.delete(remote_filename)
                            drop_metrics(metrics_filename, metrics_local_path)
                            break
            else:
                continue
        else:
            drop_metrics(metrics_filename, metrics_local_path)
        ftp.quit()
        archive_metrics(metrics_filename)
        os.remove(metrics_local_path)
        with open(log_file, 'a') as f:
            f.write(f"{get_date_time_prefix()} Connection closed - local metrics file deleted.\n")
        send_success_email()

    def grab_metrics():
        r = requests.get(url, headers=headers, cookies=cookies, allow_redirects=True)
        filename_match = re.search(filename_regex, r.url)
        filename = filename_match.group(1)
        file_path = Path(f'{local_metrics_dir}/{filename}')
        with open(file_path, 'wb') as f:
            f.write(r.content)
        with open(log_file, 'a') as f:
            f.write(f"{get_date_time_prefix()} Metrics file downloaded: {filename}\n")
        check_and_transfer(filename, file_path)
    
    h = requests.head(url, headers=headers, cookies=cookies, allow_redirects=True)
    content_type = h.headers.get('content-type').lower()
    if content_type == 'text/csv':
        grab_metrics()
    else:
        with open(log_file, 'a') as f:
            f.write(f"{get_date_time_prefix()} Metrics file could not be downloaded. Update cookies in HTTP request.\n")
        send_failure_email()
    

if __name__ == '__main__':
    main()