License:        BSD
Vendor:         Otus
Group:          PD01
URL:            http://otus.ru/lessons/3/
Name:           ip2w
Version:        0.0.1
Release:        1
BuildArch:      noarch
BuildRoot:      %{_tmppath}/%{name}-root
Requires(post): systemd
Requires(preun): systemd
Requires(postun): systemd
BuildRequires: systemd
Requires: python
Summary:  UWSGI app "ip2w"

%description
The program allows you to get the temperature for a specified IP address.


%define __etcdir    /usr/local/etc/%{name}/
%define __logdir    /var/log/%{name}/
%define __bindir    /usr/local/%{name}/
%define __systemddir	/usr/etc/systemd/system/

%prep


%install
[ "%{buildroot}" != "/" ] && rm -fr %{buildroot}
%{__mkdir} -p %{buildroot}/%{__bindir}
%{__mkdir} -p %{buildroot}/%{__etcdir}
%{__mkdir} -p %{buildroot}/%{__logdir}
%{__mkdir} -p %{buildroot}/%{__systemddir}
%{__install} -pD -m 755 wsgi.py %{buildroot}/%{__bindir}/wsgi.py
%{__install} -pD -m 644 uwsgi.ini %{buildroot}/%{__etcdir}/uwsgi.ini
%{__install} -pD -m 644 secret.json %{buildroot}/%{__etcdir}/secret.json
%{__install} -pD -m 644 ip2w.service %{buildroot}/%{__systemddir}/%{name}.service


%post
%systemd_post %{name}.service
systemctl daemon-reload

%preun
%systemd_preun %{name}.service

%postun
%systemd_postun %{name}.service

%clean
[ "%{buildroot}" != "/" ] && rm -fr %{buildroot}


%files
%{__bindir}/wsgi.py
%{__systemddir}/%{name}.service
%{__etcdir}/uwsgi.ini
%{__etcdir}/secret.json

%exclude
%{__bindir}/*.pyc
%{__bindir}/*.pyo

