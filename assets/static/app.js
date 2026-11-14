(function () {
  var breadcrumb = document.getElementById('breadcrumb');
  var fileList = document.getElementById('file-list');
  var loading = document.getElementById('loading');
  var errorDiv = document.getElementById('error');

  // hash 中保存的是已编码路径，导航时原样透传，避免二次编码
  function currentPath() {
    return location.hash.slice(1);
  }

  function navigate(path) {
    location.hash = path;
  }

  function joinPath(prefix, name) {
    return prefix ? prefix + '/' + encodeURIComponent(name) : encodeURIComponent(name);
  }

  function loadListing(path) {
    loading.style.display = 'block';
    errorDiv.style.display = 'none';
    fileList.innerHTML = '';

    var url = '/api/list' + (path ? '/' + path : '');
    fetch(url)
      .then(function (res) {
        if (!res.ok) throw new Error('HTTP ' + res.status);
        return res.json();
      })
      .then(function (data) {
        render(data, path);
      })
      .catch(function (err) {
        loading.style.display = 'none';
        errorDiv.textContent = '加载失败: ' + err.message;
        errorDiv.style.display = 'block';
      });
  }

  function render(data, encodedPath) {
    loading.style.display = 'none';

    renderBreadcrumb(data.path, encodedPath);

    var entries = data.entries;
    if (entries.length === 0) {
      var row = fileList.insertRow();
      row.className = 'empty-row';
      var cell = row.insertCell();
      cell.colSpan = 4;
      cell.textContent = '空目录';
      return;
    }

    var port = data.port;
    var lanIp = data.lan_ip;
    var prefix = encodedPath;

    var frag = document.createDocumentFragment();

    entries.forEach(function (entry) {
      var row = document.createElement('tr');
      var isDir = entry.type === 'dir';
      var isLink = entry.type === 'link';

      var nameCell = document.createElement('td');
      row.appendChild(nameCell);
      nameCell.className = 'name-cell ' + entry.type;
      var icon = document.createElement('span');
      icon.className = isDir ? 'dir-icon' : (isLink ? 'link-icon' : 'file-icon');

      var fp = joinPath(prefix, entry.name);

      if (isDir) {
        nameCell.appendChild(icon);
        nameCell.appendChild(document.createTextNode(' ' + entry.name));
        nameCell.onclick = function () { navigate(fp); };
      } else {
        var link = document.createElement('a');
        link.href = '/files/' + fp;
        link.download = entry.name;
        link.appendChild(icon);
        link.appendChild(document.createTextNode(' ' + entry.name));
        nameCell.appendChild(link);
        if (isLink) {
          // 列表里只展示链接自身的元数据，明确标出这是符号链接；下载会跟随到原始文件
          var badge = document.createElement('span');
          badge.className = 'symlink-badge';
          badge.textContent = '符号链接';
          badge.title = '下载时传输被链接的原始文件（支持多级链接）';
          nameCell.appendChild(badge);
        }
      }

      var sizeCell = document.createElement('td');
      row.appendChild(sizeCell);
      sizeCell.className = 'size-col';
      sizeCell.textContent = entry.size != null ? humanSize(entry.size) : '—';

      var modCell = document.createElement('td');
      row.appendChild(modCell);
      modCell.className = 'modified-col';
      modCell.textContent = entry.modified ? entry.modified.replace('T', ' ') : '—';

      var linkCell = document.createElement('td');
      row.appendChild(linkCell);
      linkCell.className = 'link-group';

      if (isDir) {
        var zbase = '/api/zip/' + fp;
        var dl = document.createElement('a');
        dl.className = 'dl-link';
        dl.href = zbase;
        dl.download = entry.name + '.zip';
        dl.textContent = '下载';
        linkCell.appendChild(dl);
        if (lanIp) {
          linkCell.appendChild(makeCopyBtn('zip局域网', 'http://' + lanIp + ':' + port + zbase));
        }
        linkCell.appendChild(makeCopyBtn('zip本地', 'http://127.0.0.1:' + port + zbase));
      } else {
        // 符号链接也按文件给直链：/files 会跟随链接传输原始文件
        var base = '/files/' + fp;
        if (lanIp) {
          linkCell.appendChild(makeCopyBtn('局域网', 'http://' + lanIp + ':' + port + base));
        }
        linkCell.appendChild(makeCopyBtn('本地', 'http://127.0.0.1:' + port + base));
      }

      frag.appendChild(row);
    });

    fileList.appendChild(frag);
  }

  function renderBreadcrumb(decodedPath, encodedPath) {
    breadcrumb.innerHTML = '';
    var home = document.createElement('a');
    home.textContent = '🏠 首页';
    home.onclick = function () { navigate(''); };
    breadcrumb.appendChild(home);

    if (decodedPath && decodedPath !== '/') {
      var parts = decodedPath.slice(1).split('/');
      var encParts = encodedPath.split('/');
      var acc = '';
      parts.forEach(function (part, i) {
        acc += (i > 0 ? '/' : '') + encParts[i];
        var sep = document.createElement('span');
        sep.className = 'sep';
        sep.textContent = '/';
        breadcrumb.appendChild(sep);
        var link = document.createElement('a');
        link.textContent = part;
        var target = acc;
        link.onclick = function () { navigate(target); };
        breadcrumb.appendChild(link);
      });
    }
  }

  function makeCopyBtn(label, text) {
    var btn = document.createElement('button');
    btn.className = 'copy-btn';
    btn.textContent = label;
    btn.onclick = function () { copyText(text, btn); };
    return btn;
  }

  function humanSize(bytes) {
    var units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var i = 0;
    while (bytes >= 1024 && i < units.length - 1) {
      bytes /= 1024;
      i++;
    }
    return (i === 0 ? Math.round(bytes) : bytes.toFixed(1)) + ' ' + units[i];
  }

  function copyText(text, btn) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(function () {
        showCopied(btn);
      });
    } else {
      var ta = document.createElement('textarea');
      ta.value = text;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      try { document.execCommand('copy'); showCopied(btn); } catch (e) {}
      document.body.removeChild(ta);
    }
  }

  function showCopied(btn) {
    var original = btn.textContent;
    btn.classList.add('copied');
    btn.textContent = '已复制';
    setTimeout(function () {
      btn.classList.remove('copied');
      btn.textContent = original;
    }, 1500);
  }

  window.addEventListener('hashchange', function () {
    loadListing(currentPath());
  });

  loadListing(currentPath());
})();
