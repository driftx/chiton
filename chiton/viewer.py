from twisted.internet import reactor, defer
from telephus.client import CassandraClient
from telephus.protocol import ManagedCassandraClientFactory
from telephus.cassandra.ttypes import *
import simplejson, collections, os
json_encode = simplejson.dumps
json_decode = simplejson.loads

try: 
    import pygtk 
    pygtk.require("2.0") 
except: 
    pass 
import gtk 
import gtk.glade 
    
class ChitonViewer(object): 
    def __init__(self, host=None, port=None): 
        self.gladefile = os.path.join(os.path.dirname(__file__), "chiton.glade")
        self.cmanager = None
        self._client = None
        self._ksmap = {}
        self._currentks = None
        self._currentcf = None
        self._pageamt = 25
        self._maxcomphist = 100
        self._lastcol = None
        self.columns = None
        self.keyspaces = None
            
        self.windowname = "mainWindow" 
        self.wTree = gtk.glade.XML(self.gladefile, self.windowname) 
        self.window = self.wTree.get_widget(self.windowname)
        self.columnsView = self.wTree.get_widget("columnsView")
        self.keyspaceView = self.wTree.get_widget("keyspaceView")
        
        self._loadprefs()
        self.columnEntry = self.wTree.get_widget("columnEntry")
        self.columnCompletionStore = self._setCompletion(self.columnEntry,
                                     self._prefs['completion']['columnEntry'])
        self.rowEntry = self.wTree.get_widget("rowEntry")
        self.rowCompletionStore = self._setCompletion(self.rowEntry,
                                    self._prefs['completion']['rowEntry'])
        
        self.columnLabel = self.wTree.get_widget("columnLabel")
        self.entryTable = self.wTree.get_widget("entryTable")
        self.goButton = self.wTree.get_widget("goButton")
        self.pageToolbar = self.wTree.get_widget("pageToolbar")
        self.pagePrev = self.wTree.get_widget("prevbutton")
        self.pageNext = self.wTree.get_widget("nextbutton")
        self.statusbar = self.wTree.get_widget("statusbar")
        self.sid = self.statusbar.get_context_id("status")
        self.statusbar.push(self.sid, '')

        self.goButton.connect("clicked", self.updateView)
        self.pageNext.connect("clicked", self.nextPage)
        self.pagePrev.connect("clicked", self.prevPage)
        self.keyspaceView.get_selection().connect('changed', self.keyspaceChanged)
        self.wTree.get_widget("quitmenuitem").connect("activate", self._quit)
        self.wTree.get_widget("connectmenuitem").connect("activate", self._connectDialog)
        
        self._resetpages()
        
        self.wTree.signal_autoconnect({
            "on_mainWindow_destroy": self._quit,
        })
        self.window.show()
        if host and port:
            self._connect(host, port)
        
    def _quit(self, res=None):
        if self.cmanager:
            self.cmanager.shutdown()
        try:
            open(self.prefpath, 'w').write(json_encode(self._prefs))
        except Exception, e:
            print e
        reactor.stop()
        return False
   
    def _prefpath(self):
        return os.path.join(os.path.expanduser('~'), '.chiton.json')
    prefpath = property(_prefpath)
    
    def _loadprefs(self):
        self._prefs = {}
        try:
            self._prefs = json_decode(open(self.prefpath).read())
        except Exception, e:
            print e
        def ldict():
            return collections.defaultdict(list)
        if not self._prefs:
            self._prefs = collections.defaultdict(ldict)
    
    def _resetpages(self):
        self._currpage = 1
        self._firstcol = ''
        self._lastcol = ''
        self._lastrow = None
        
    def _setCompletion(self, entry, data):
        completer = gtk.EntryCompletion()
        store = gtk.ListStore(str)
        completer.set_model(store)
        completer.set_text_column(0)
        entry.set_completion(completer)
        for item in data:
            store.append([item])
        return store
    
    def _updateCompletion(self):
        row = self.rowEntry.get_text()
        column = self.columnEntry.get_text()
        if row not in self._prefs['completion']['rowEntry']:
            self.rowCompletionStore.append([row])
            self._prefs['completion']['rowEntry'].append(row)
        if column not in self._prefs['completion']['columnEntry']:
            self.columnCompletionStore.append([column])
            self._prefs['completion']['columnEntry'].append(column)
        for k in ('rowEntry', 'columnEntry'):
            if len(self._prefs['completion'][k]) > self._maxcomphist:
                self._prefs['completion'][k].pop(0)
        
    def _addcol(self, view, name, colId, width=None):
        col = gtk.TreeViewColumn(name, gtk.CellRendererText(), text=colId)
        col.set_resizable(True)
        if width:
            col.set_fixed_width(width)
        col.set_sort_column_id(colId)
        view.append_column(col)
        
    def _status(self, status):
        self.statusbar.pop(self.sid)
        self.statusbar.push(self.sid, status)

    def _showError(self, err):
        errTree = gtk.glade.XML(self.gladefile, "errorDialog")
        errorDialog = errTree.get_widget("errorDialog")
        errorDialog.set_markup(str(err))
        errorDialog.run()
        errorDialog.destroy()
        
    @defer.inlineCallbacks
    def _connect(self, host, port):
        try:
            if self.cmanager:
                self.cmanager.shutdown()
            self.cmanager = ManagedCassandraClientFactory()
            print "connecting..."
            for x in xrange(3):
                reactor.connectTCP(host, int(port), self.cmanager)
            yield self.cmanager.deferred
            yield self._setupKeyspaces()
            self._setupColumns()
        except Exception, e:
            if self.cmanager:
                self.cmanager.shutdown()
                self.cmanager = None
            self._status(str(e))
            self._showError(e)
            raise
        defer.returnValue(None)
        
    @defer.inlineCallbacks
    def _connectDialog(self, source=None):
        cdlg = ConnectDialog(self.gladefile)
        result, host, port = cdlg.run()
        if result == 0:
            yield self._connect(host, port)
        
    @defer.inlineCallbacks
    def _setupKeyspaces(self):
        if self.keyspaces:
            self.keyspaces.clear()
            for c in self.keyspaceView.get_columns():
                self.keyspaceView.remove_column(c)
        self._addcol(self.keyspaceView, 'Keyspaces', 0, width=20)
        self.keyspaces = gtk.TreeStore(str)
        self.keyspaceView.set_model(self.keyspaces)
        c = CassandraClient(self.cmanager, '')
        self._status("Fetching keyspaces...")
        ks = yield c.get_string_list_property('keyspaces')
        self._status("Found %s keyspaces" % len(ks))
        for i,k in enumerate(ks):
            if k != 'system':
                self.keyspaces.append(None, [k])
                kiter = self.keyspaces.get_iter(str(i))
                self._status("Describing keyspace '%s'..." % k)
                r = yield c.describe_keyspace(k)
                self._status("Received description of keyspace '%s':"""
                             "%s column families" % (k, len(r)))
                self._ksmap[k] = r
                print r
                for col, info in r.items():
                    self.keyspaces.append(kiter, [col])
       
    def _setupColumns(self):
        if self.columns:
            self.columns.clear()
            for c in self.columnsView.get_columns():
                self.columnsView.remove_column(c)
        self._addcol(self.columnsView, 'Column name', 0)
        self._addcol(self.columnsView, 'Value', 1)
        self.columns = gtk.ListStore(str, str)
        self.columnsView.set_model(self.columns)
                
    def keyspaceChanged(self, selection):
        self._resetpages()
        tree, path = selection.get_selected_rows()
        if path:
            if len(path[0]) == 1:
                self._currentks = tree[path[0]][0]
                self._currentcf = None
                self.entryTable.hide()
            elif len(path[0]) == 2:
                self._currentks = tree[(path[0][0],)][0]
                self._currentcf = tree[path[0]][0]
                self.entryTable.show()
            self.columns.clear()
        if self._currentcf:
            self._client = CassandraClient(self.cmanager, self._currentks)
            cf = self._ksmap[self._currentks][self._currentcf]
            if cf['Type'] == 'Super':
                self._status("Column family '%s': Type: %s, CompareWith: %s, """
                             "CompareSubWith: %s" % (self._currentcf, cf['Type'],
                                cf['CompareWith'], cf['CompareSubcolumnsWith']))
                self.columnEntry.show()
                self.columnLabel.show()
            else:
                self._status("Column family '%s': Type: %s, CompareWith: %s """
                             % (self._currentcf, cf['Type'], cf['CompareWith']))
                self.columnEntry.hide()
                self.columnLabel.hide()

    @defer.inlineCallbacks
    def updateView(self, source=None, start='', reverse=False):
        if source == self.goButton:
            self._resetpages()
            self._updateCompletion()
        try:
            if self._ksmap[self._currentks][self._currentcf]['Type'] == 'Super':
                path = ColumnParent(column_family=self._currentcf,
                                    super_column=self.columnEntry.get_text())
            else:
                path = ColumnParent(column_family=self._currentcf)
            self._status("Fetching data...")
            cols = yield self._client.get_slice(self.rowEntry.get_text(), path,
                count=self._pageamt, start=start, reverse=reverse)
            self._status("%s columns retrieved" % len(cols))
            self.columns.clear()
            if reverse:
                cols.reverse()
            for col in cols:
                self.columns.append([col.column.name, col.column.value])
            if cols:
                self._firstcol = cols[0].column.name
                self._lastcol = cols[-1].column.name
            if self._lastrow == self.rowEntry.get_text():
                if reverse:
                    self._currpage -= 1
                else:
                    self._currpage += 1
            self._lastrow = self.rowEntry.get_text()
            if self._currpage > 1:
                self.pagePrev.set_property('sensitive', True)
            else:
                self.pagePrev.set_property('sensitive', False)
            if len(cols) >= self._pageamt:
                self.pageNext.set_property('sensitive', True)
            else:
                self.pageNext.set_property('sensitive', False)
        except Exception, e:
            self._showError(e)
            raise
            
    def nextPage(self, source):
        self.updateView(start=self._lastcol)
        
    def prevPage(self, source):
        self.updateView(start=self._firstcol, reverse=True)

class ConnectDialog(object):
    def __init__(self, gladefile):
        self.wTree = gtk.glade.XML(gladefile, "connectDialog")
        self.dialog = self.wTree.get_widget("connectDialog")
        self.hostEntry = self.wTree.get_widget("hostEntry")
        self.portEntry = self.wTree.get_widget("portEntry")
        
    def run(self):
        self.result = self.dialog.run()
        self.dialog.destroy()
        return self.result, self.hostEntry.get_text(), self.portEntry.get_text()