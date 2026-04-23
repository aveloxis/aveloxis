## Production Setup Guide

## Proxying Augur through Nginx
Assumes nginx is installed. 

Then you create a file for the server you want Augur to run under in the location of your `sites-enabled` directory for nginx. In this example, Augur is running on port 5555: (the long timeouts on the settings page is for when a user adds a large number of repos or orgs in a single session to prevent timeouts from nginx)

```
server {
        server_name  hans.aveloxis.io;

        location /api/unstable/ {
                proxy_pass http://hans.aveloxis.io:5555;
                proxy_set_header Host $host;
        }

        location / {
                proxy_pass http://127.0.0.1:5555;
        }

        location /settings {

                proxy_read_timeout 800;
                proxy_connect_timeout 800;
                proxy_send_timeout 800;
        }

        error_log /var/log/nginx/augurview.osshealth.error.log;
        access_log /var/log/nginx/augurview.osshealth.access.log;

}

```

### Setting up SSL (https)
Install Certbot: 
```
sudo apt update &&
sudo apt upgrade &&
sudo apt install certbot &&
sudo apt-get install python3-certbot-nginx
```

Generate a certificate for the specific domain for which you have a file already in the sites-enabled directory for nginx (located at `/etc/nginx/sites-enabled` on Ubuntu): 
```
 sudo certbot -v --nginx  -d hans.aveloxis.io
```

In the example file above. Your resulting nginx sites-enabled file will look like this: 

```
server {
        server_name  hans.aveloxis.io;

        location /api/unstable/ {
                proxy_pass http://hans.aveloxis.io:5555;
                proxy_set_header Host $host;
        }

   location / {
      proxy_pass http://127.0.0.1:5555;
   }

   location /settings {

                proxy_read_timeout 800;
                proxy_connect_timeout 800;
                proxy_send_timeout 800;
   }

        error_log /var/log/nginx/augurview.osshealth.error.log;
        access_log /var/log/nginx/augurview.osshealth.access.log;

    listen 443 ssl; # managed by Certbot
    ssl_certificate /etc/letsencrypt/live/hans.aveloxis.io/fullchain.pem; # managed by Certbot
    ssl_certificate_key /etc/letsencrypt/live/hans.aveloxis.io/privkey.pem; # managed by Certbot
    include /etc/letsencrypt/options-ssl-nginx.conf; # managed by Certbot
    ssl_dhparam /etc/letsencrypt/ssl-dhparams.pem; # managed by Certbot

}

server {
    if ($host = hans.aveloxis.io) {
        return 301 https://$host$request_uri;
    } # managed by Certbot


        listen 80;
        server_name  hans.aveloxis.io;
    return 404; # managed by Certbot


}
```
