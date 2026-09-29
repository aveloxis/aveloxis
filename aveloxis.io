server {
    listen 443 ssl;
    server_name aveloxis.io;

    ssl_certificate     /etc/letsencrypt/live/aveloxis.io/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/aveloxis.io/privkey.pem;
    include             /etc/letsencrypt/options-ssl-nginx.conf;
    ssl_dhparam         /etc/letsencrypt/ssl-dhparams.pem;

    root /var/www/aveloxis-gui;
    index index.html;

    include /etc/nginx/snippets/shared-error-pages.conf;
    include /etc/nginx/snippets/maintenance-handler.conf;

    access_log /var/log/nginx/aveloxis.access.log;
    error_log  /var/log/nginx/aveloxis.error.log;

    location / {
        include /etc/nginx/snippets/maintenance-check.conf;
        try_files $uri $uri.html $uri/ =404;
    }
    location /api/ {
        proxy_pass http://127.0.0.1:8383;
        # APPEND the real client IP — the API reads the RIGHTMOST
        # X-Forwarded-For entry and believes it only from trusted_proxy.
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header Host $host;
    }
    location /auth/ {
        proxy_pass http://127.0.0.1:8082;
        proxy_set_header Host $host;   # OAuth callbacks + cookies need
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
	#try_files $uri $uri.html $uri/ =404;
    }
    location /a/ {
    proxy_pass http://127.0.0.1:3000/;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_http_version 1.1;
    }
}

server {
    listen 80;
    server_name aveloxis.io;

    location /.well-known/acme-challenge/ {
        root /var/www/html;
    }

    location / {
        return 301 https://aveloxis.io$request_uri;
    }
}
