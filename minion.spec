Name: minion
Version: 0.7.0
Release: 1%{?dist}
Summary: Alibaba P4P library and cli

Group: Alibaba/OPS
License: Alibaba
URL: http://sam.alibaba-inc.com
Source0: minion.tar.gz
BuildRoot: %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

Buildarch: noarch

BuildRequires: python >= 2.7

Requires: python >= 2.7
Requires: python-requests

%description
Alibaba P4P library and cli, implemented by pure python.

%prep
%setup -q -c

%build
python setup.py

%install
rm -rf $RPM_BUILD_ROOT
cp minion.py %{buildroot}/usr/bin/
ln -s /usr/bin/minion.py %{buildroot}/usr/bin/minion

%files
%_prefix
/usr/bin/minion
/usr/bin/minion.py

%changelog
* Thu Mar 5 2015 shi yu <linxiulei@gmail.com>
- build in the obs
