# script auto-generated by phenix.molprobity


from __future__ import division
import cPickle
try :
  import gobject
except ImportError :
  gobject = None
import sys

class coot_extension_gui (object) :
  def __init__ (self, title) :
    import gtk
    self.window = gtk.Window(gtk.WINDOW_TOPLEVEL)
    scrolled_win = gtk.ScrolledWindow()
    self.outside_vbox = gtk.VBox(False, 2)
    self.inside_vbox = gtk.VBox(False, 0)
    self.window.set_title(title)
    self.inside_vbox.set_border_width(0)
    self.window.add(self.outside_vbox)
    self.outside_vbox.pack_start(scrolled_win, True, True, 0)
    scrolled_win.add_with_viewport(self.inside_vbox)
    scrolled_win.set_policy(gtk.POLICY_AUTOMATIC, gtk.POLICY_AUTOMATIC)

  def finish_window (self) :
    import gtk
    self.outside_vbox.set_border_width(2)
    ok_button = gtk.Button("  Close  ")
    self.outside_vbox.pack_end(ok_button, False, False, 0)
    ok_button.connect("clicked", lambda b: self.destroy_window())
    self.window.connect("delete_event", lambda a, b: self.destroy_window())
    self.window.show_all()

  def destroy_window (self, *args) :
    self.window.destroy()
    self.window = None

  def confirm_data (self, data) :
    for data_key in self.data_keys :
      outlier_list = data.get(data_key)
      if outlier_list is not None and len(outlier_list) > 0 :
        return True
    return False

  def create_property_lists (self, data) :
    import gtk
    for data_key in self.data_keys :
      outlier_list = data[data_key]
      if outlier_list is None or len(outlier_list) == 0 :
        continue
      else :
        frame = gtk.Frame(self.data_titles[data_key])
        vbox = gtk.VBox(False, 2)
        frame.set_border_width(6)
        frame.add(vbox)
        self.add_top_widgets(data_key, vbox)
        self.inside_vbox.pack_start(frame, False, False, 5)
        list_obj = residue_properties_list(
          columns=self.data_names[data_key],
          column_types=self.data_types[data_key],
          rows=outlier_list,
          box=vbox)

# Molprobity result viewer
class coot_molprobity_todo_list_gui (coot_extension_gui) :
  data_keys = [ "rama", "rota", "cbeta", "probe" ]
  data_titles = { "rama"  : "Ramachandran outliers",
                  "rota"  : "Rotamer outliers",
                  "cbeta" : "C-beta outliers",
                  "probe" : "Severe clashes" }
  data_names = { "rama"  : ["Chain", "Residue", "Name", "Score"],
                 "rota"  : ["Chain", "Residue", "Name", "Score"],
                 "cbeta" : ["Chain", "Residue", "Name", "Conf.", "Deviation"],
                 "probe" : ["Atom 1", "Atom 2", "Overlap"] }
  if (gobject is not None) :
    data_types = { "rama" : [gobject.TYPE_STRING, gobject.TYPE_STRING,
                             gobject.TYPE_STRING, gobject.TYPE_FLOAT,
                             gobject.TYPE_PYOBJECT],
                   "rota" : [gobject.TYPE_STRING, gobject.TYPE_STRING,
                             gobject.TYPE_STRING, gobject.TYPE_FLOAT,
                             gobject.TYPE_PYOBJECT],
                   "cbeta" : [gobject.TYPE_STRING, gobject.TYPE_STRING,
                              gobject.TYPE_STRING, gobject.TYPE_STRING,
                              gobject.TYPE_FLOAT, gobject.TYPE_PYOBJECT],
                   "probe" : [gobject.TYPE_STRING, gobject.TYPE_STRING,
                              gobject.TYPE_FLOAT, gobject.TYPE_PYOBJECT] }
  else :
    data_types = dict([ (s, []) for s in ["rama","rota","cbeta","probe"] ])

  def __init__ (self, data_file=None, data=None) :
    assert ([data, data_file].count(None) == 1)
    if (data is None) :
      data = load_pkl(data_file)
    if not self.confirm_data(data) :
      return
    coot_extension_gui.__init__(self, "MolProbity to-do list")
    self.dots_btn = None
    self.dots2_btn = None
    self._overlaps_only = True
    self.window.set_default_size(420, 600)
    self.create_property_lists(data)
    self.finish_window()

  def add_top_widgets (self, data_key, box) :
    import gtk
    if data_key == "probe" :
      hbox = gtk.HBox(False, 2)
      self.dots_btn = gtk.CheckButton("Show Probe dots")
      hbox.pack_start(self.dots_btn, False, False, 5)
      self.dots_btn.connect("toggled", self.toggle_probe_dots)
      self.dots2_btn = gtk.CheckButton("Overlaps only")
      hbox.pack_start(self.dots2_btn, False, False, 5)
      self.dots2_btn.connect("toggled", self.toggle_all_probe_dots)
      self.dots2_btn.set_active(True)
      self.toggle_probe_dots()
      box.pack_start(hbox, False, False, 0)

  def toggle_probe_dots (self, *args) :
    if self.dots_btn is not None :
      show_dots = self.dots_btn.get_active()
      overlaps_only = self.dots2_btn.get_active()
      if show_dots :
        self.dots2_btn.set_sensitive(True)
      else :
        self.dots2_btn.set_sensitive(False)
      show_probe_dots(show_dots, overlaps_only)

  def toggle_all_probe_dots (self, *args) :
    if self.dots2_btn is not None :
      self._overlaps_only = self.dots2_btn.get_active()
      self.toggle_probe_dots()

class rsc_todo_list_gui (coot_extension_gui) :
  data_keys = ["by_res", "by_atom"]
  data_titles = ["Real-space correlation by residue",
                 "Real-space correlation by atom"]
  data_names = {}
  data_types = {}

class residue_properties_list (object) :
  def __init__ (self, columns, column_types, rows, box,
      default_size=(380,200)) :
    assert len(columns) == (len(column_types) - 1)
    if (len(rows) > 0) and (len(rows[0]) != len(column_types)) :
      raise RuntimeError("Wrong number of rows:\n%s" % str(rows[0]))
    import gtk
    self.liststore = gtk.ListStore(*column_types)
    self.listmodel = gtk.TreeModelSort(self.liststore)
    self.listctrl = gtk.TreeView(self.listmodel)
    self.listctrl.column = [None]*len(columns)
    self.listctrl.cell = [None]*len(columns)
    for i, column_label in enumerate(columns) :
      cell = gtk.CellRendererText()
      column = gtk.TreeViewColumn(column_label)
      self.listctrl.append_column(column)
      column.set_sort_column_id(i)
      column.pack_start(cell, True)
      column.set_attributes(cell, text=i)
    self.listctrl.get_selection().set_mode(gtk.SELECTION_SINGLE)
    for row in rows :
      self.listmodel.get_model().append(row)
    self.listctrl.connect("cursor-changed", self.OnChange)
    sw = gtk.ScrolledWindow()
    w, h = default_size
    if len(rows) > 10 :
      sw.set_size_request(w, h)
    else :
      sw.set_size_request(w, 30 + (20 * len(rows)))
    sw.set_policy(gtk.POLICY_AUTOMATIC, gtk.POLICY_AUTOMATIC)
    box.pack_start(sw, False, False, 5)
    inside_vbox = gtk.VBox(False, 0)
    sw.add(self.listctrl)

  def OnChange (self, treeview) :
    import coot # import dependency
    selection = self.listctrl.get_selection()
    (model, tree_iter) = selection.get_selected()
    if tree_iter is not None :
      row = model[tree_iter]
      xyz = row[-1]
      if isinstance(xyz, tuple) and len(xyz) == 3 :
        set_rotation_centre(*xyz)
        set_zoom(30)
        graphics_draw()

def show_probe_dots (show_dots, overlaps_only) :
  import coot # import dependency
  n_objects = number_of_generic_objects()
  sys.stdout.flush()
  if show_dots :
    for object_number in range(n_objects) :
      obj_name = generic_object_name(object_number)
      if overlaps_only and not obj_name in ["small overlap", "bad overlap"] :
        sys.stdout.flush()
        set_display_generic_object(object_number, 0)
      else :
        set_display_generic_object(object_number, 1)
  else :
    sys.stdout.flush()
    for object_number in range(n_objects) :
      set_display_generic_object(object_number, 0)

def load_pkl (file_name) :
  pkl = open(file_name, "rb")
  data = cPickle.load(pkl)
  pkl.close()
  return data

data = {}
data['rama'] = [('A', '  66 ', 'PRO', 0.029909434915612754, (-13.997, -7.672999999999997, -27.090999999999994)), ('B', '  66 ', 'PRO', 0.038390268758393624, (-1.491000000000001, 14.059999999999999, -17.029)), ('C', '  66 ', 'PRO', 0.03827420631680972, (-35.85000000000001, 27.976, -38.171))]
data['omega'] = []
data['rota'] = [('A', '  14 ', 'LYS', 0.0, (15.639000000000005, -0.34499999999999986, -2.263)), ('A', '  38 ', 'ASP', 0.15246760441038493, (11.061999999999998, 1.9649999999999999, 3.337)), ('A', '  43 ', 'THR', 0.0258387386566028, (10.459, -0.5280000000000007, 8.869)), ('A', '  70 ', 'ARG', 0.049616127278889074, (-19.232, -6.8249999999999975, -38.707)), ('A', ' 191 ', 'LEU', 0.0006283375928322313, (-25.067999999999998, 2.501999999999998, -12.605999999999998)), ('A', ' 191 ', 'LEU', 0.0006283375928322313, (-25.067999999999998, 2.501999999999998, -12.605999999999998)), ('A', ' 191 ', 'LEU', 0.0028361563033514445, (-25.029, 2.348, -12.295)), ('A', ' 191 ', 'LEU', 0.0028361563033514445, (-25.029, 2.348, -12.295)), ('B', '  31 ', 'LEU', 0.0, (-13.768000000000002, -7.039, 5.158999999999999)), ('B', '  38 ', 'ASP', 0.18060995709563402, (-30.27499999999999, -12.751, -6.378)), ('B', '  43 ', 'THR', 0.021758937509730458, (-30.465, -15.176, -0.887)), ('B', ' 208 ', 'ASN', 0.23591093133194632, (-9.613, 4.374999999999998, -35.631)), ('B', ' 208 ', 'ASN', 0.23591093133194632, (-9.613, 4.374999999999998, -35.631)), ('C', '  29 ', 'VAL', 0.24066820247977172, (-19.269, 21.015000000000004, -59.76099999999999)), ('C', '  31 ', 'LEU', 0.0, (-19.407, 14.904000000000002, -63.307)), ('C', '  53 ', 'THR', 0.11066816975558555, (-15.898999999999997, 22.889000000000003, -52.68799999999999)), ('C', '  53 ', 'THR', 0.11066816975558555, (-15.898999999999997, 22.889000000000003, -52.68799999999999)), ('C', ' 164 ', 'ASP', 0.012362709378993, (-22.957, 40.295, -52.93999999999999)), ('D', '  14 ', 'LYS', 0.27569302215390645, (-37.394000000000005, 40.38499999999999, -61.749999999999986)), ('D', '  23 ', 'ILE', 0.27329494225913686, (-20.579, 21.93099999999999, -71.057)), ('D', '  31 ', 'LEU', 0.03224889406578065, (-23.270000000000003, 20.104, -66.98)), ('D', '  31 ', 'LEU', 0.03229100311363206, (-23.268, 20.101999999999997, -66.981)), ('D', '  43 ', 'THR', 0.0258387386566028, (-25.027, 39.661, -65.533)), ('D', '  45 ', 'THR', 0.00839585782904849, (-23.11, 32.952999999999996, -64.185)), ('D', '  70 ', 'ARG', 0.03754404954827152, (-53.49799999999998, -2.3169999999999993, -39.959)), ('D', '  72 ', 'LEU', 0.04187032887812, (-55.406, -2.255999999999998, -33.889)), ('D', ' 149 ', 'ASN', 0.28210413348629976, (-54.01400000000001, -1.5409999999999995, -46.307)), ('D', ' 164 ', 'ASP', 0.29633975466385937, (-32.397999999999996, -5.774999999999997, -63.697)), ('D', ' 208 ', 'ASN', 0.006061527967487663, (-48.19999999999998, 20.006, -32.315)), ('D', ' 208 ', 'ASN', 0.006061527967487663, (-48.19999999999998, 20.006, -32.315))]
data['cbeta'] = [('A', '  38 ', 'ASP', ' ', 0.26392060134331213, (10.251000000000001, 3.021, 4.128)), ('A', '  82 ', 'GLN', ' ', 0.2515268797362573, (-22.838, -7.988999999999999, -13.229)), ('B', '  82 ', 'GLN', ' ', 0.2825302827676934, (5.1690000000000005, 4.027, -5.699))]
data['probe'] = [(' C  55 CLYS  CB ', ' C  57 CGLN  HG3', -0.858, (-14.057, 20.539, -46.324)), (' C  55 DLYS  CB ', ' C  57 DGLN  HG3', -0.858, (-14.057, 20.539, -46.324)), (' A 104  THR HG23', ' A 107  ALA  H  ', -0.845, (-5.441, -19.102, -22.57)), (' C 125  GLU  OE1', ' C 403  HOH  O  ', -0.797, (-35.375, 18.165, -33.281)), (' D 150  GLY  O  ', ' D 401  HOH  O  ', -0.742, (-51.306, -6.521, -44.228)), (' D 120  LYS  H  ', ' D 155  ASN HD21', -0.714, (-49.638, -4.151, -51.381)), (' D 207 BALA  O  ', ' D 208 BASN  HB2', -0.702, (-47.22, 17.868, -32.159)), (' D 207 AALA  O  ', ' D 208 AASN  HB2', -0.702, (-47.22, 17.868, -32.159)), (' C 104  THR HG23', ' C 107  ALA  H  ', -0.695, (-20.959, 25.13, -36.651)), (' C 403  HOH  O  ', ' D 206  HIS  HE1', -0.667, (-36.072, 18.778, -33.569)), (' A 203  ALA  HB3', ' B 203  ALA  HB3', -0.666, (-7.293, 4.256, -25.107)), (' D 188  GLU  HG3', ' D 189  GLU  HG3', -0.656, (-26.498, -0.33, -37.805)), (' C 166  GLU  OE2', ' C 401  HOH  O  ', -0.647, (-26.212, 32.96, -48.406)), (' D 194  ASP  OD1', ' D 196 CARG  HD3', -0.647, (-34.689, 12.626, -49.92)), (' A  70  ARG  HD3', ' A 409  HOH  O  ', -0.646, (-20.148, -4.539, -36.009)), (' D 194  ASP  OD1', ' D 196 AARG  HD3', -0.64, (-34.328, 12.11, -49.808)), (' B  92  ILE HD11', ' B 191  LEU HD13', -0.627, (8.125, 0.603, -15.018)), (' C 203  ALA  HB3', ' D 203  ALA  HB3', -0.61, (-42.548, 16.441, -40.959)), (' C 179  LEU HD11', ' C 198  TYR  CZ ', -0.576, (-47.638, 19.423, -50.175)), (' B 104  THR HG23', ' B 107  ALA  H  ', -0.566, (-10.975, 19.486, -7.065)), (' A 167 DPHE  CE1', ' B  39  PRO  HD2', -0.566, (-29.4, -13.999, -10.106)), (' B 208 CASN  N  ', ' B 208 CASN HD22', -0.559, (-9.241, 5.899, -34.727)), (' B 208 DASN  N  ', ' B 208 DASN HD22', -0.559, (-9.241, 5.899, -34.727)), (' A 167 CPHE  CE1', ' B  39  PRO  HD2', -0.558, (-30.091, -14.312, -9.854)), (' B 190  HIS  CG ', ' B 190  HIS  O  ', -0.556, (9.371, -7.085, -12.101)), (' D 104  THR HG23', ' D 107  ALA  H  ', -0.555, (-49.568, 7.827, -62.186)), (' A 125  GLU  OE1', ' B 206  HIS  HE1', -0.548, (-5.054, -4.215, -29.439)), (' D 207 BALA  O  ', ' D 208 BASN  CB ', -0.544, (-47.22, 18.626, -32.126)), (' D 207 AALA  O  ', ' D 208 AASN  CB ', -0.544, (-47.22, 18.626, -32.126)), (' D  37  MET  SD ', ' D  41  GLY  O  ', -0.543, (-28.022, 42.711, -65.996)), (' C  15  GLN  N  ', ' C 404  HOH  O  ', -0.519, (-20.878, -5.611, -52.681)), (' A 180  GLN  HG3', ' A 468  HOH  O  ', -0.51, (-18.25, 10.455, -27.31)), (' B 301  EDO  C2 ', ' B 404  HOH  O  ', -0.509, (2.594, 23.442, -26.112)), (' C  74  TYR  HB3', ' C 426  HOH  O  ', -0.494, (-48.688, 27.856, -35.342)), (' A  71  THR HG23', ' A 151  ASP  OD2', -0.494, (-18.563, -8.621, -42.548)), (' C 203  ALA  HB2', ' D 200  TYR  CD1', -0.494, (-43.374, 13.907, -42.79)), (' B 158  PRO  O  ', ' B 160  PRO  HD3', -0.487, (10.019, 17.9, -9.025)), (' D 191  LEU  C  ', ' D 191  LEU HD12', -0.486, (-26.601, 5.392, -43.511)), (' A  54  ARG  HD3', ' A  60  ASP  OD1', -0.481, (-4.354, -13.593, -7.509)), (' C 200  TYR  OH ', ' D 206  HIS  HD2', -0.48, (-39.624, 21.109, -37.664)), (' C 132  MET  HE1', ' C 196  ARG  CZ ', -0.479, (-32.201, 20.26, -50.157)), (' A 132  MET  HE1', ' A 196  ARG  CZ ', -0.476, (-10.547, -4.131, -13.514)), (' A 179  LEU HD23', ' A 205  LYS  HD2', -0.475, (-14.518, 11.885, -24.617)), (' A 104  THR  CG2', ' A 107  ALA  H  ', -0.47, (-5.723, -19.815, -22.602)), (' B 165  GLY  HA2', ' B 167  PHE  CZ ', -0.461, (7.735, 7.159, 3.308)), (' C 132  MET  O  ', ' D 196 BARG  NH1', -0.451, (-32.275, 14.416, -51.225)), (' B  14  LYS  HE3', ' B  39  PRO  HB2', -0.45, (-33.502, -11.786, -11.529)), (' A  71  THR  O  ', ' A  73  HIS  HD2', -0.447, (-22.339, -4.498, -40.918)), (' D  71  THR  O  ', ' D  73  HIS  ND1', -0.444, (-53.954, -4.32, -36.163)), (' A 203  ALA  HB2', ' B 200  TYR  CD1', -0.442, (-6.587, 6.482, -22.785)), (' C 132  MET  O  ', ' D 196 DARG  NH1', -0.44, (-32.143, 14.638, -51.628)), (' B 301  EDO  H22', ' B 404  HOH  O  ', -0.435, (2.574, 23.568, -26.366)), (' A  68  LEU  HG ', ' A  70  ARG  HD2', -0.43, (-19.919, -6.781, -34.13)), (' A 104  THR HG22', ' A 107  ALA  CB ', -0.428, (-6.234, -21.149, -22.105)), (' D  70  ARG  HD2', ' D 404  HOH  O  ', -0.425, (-49.601, -1.604, -39.841)), (' A 206  HIS  HD2', ' B 200  TYR  OH ', -0.424, (-6.168, 10.585, -22.59)), (' A 191 BLEU  C  ', ' A 191 BLEU  CD2', -0.423, (-23.776, 2.916, -14.039)), (' A 191 ALEU  C  ', ' A 191 ALEU  CD2', -0.423, (-23.776, 2.916, -14.039)), (' A 200  TYR  CD1', ' B 203  ALA  HB2', -0.422, (-8.953, 1.682, -24.802)), (' C 206  HIS  HE1', ' D 125  GLU  OE1', -0.421, (-50.676, 13.724, -46.244)), (' D  49  VAL  O  ', ' D 137  SER  HA ', -0.417, (-29.088, 20.503, -62.353)), (' D 149  ASN  C  ', ' D 149  ASN HD22', -0.412, (-54.74, -2.951, -45.094)), (' D 149  ASN  ND2', ' D 151  ASP  H  ', -0.41, (-55.265, -3.577, -44.536)), (' C  58  THR  CB ', ' C 142  HIS  NE2', -0.409, (-21.371, 17.338, -40.919)), (' C 112  GLU  OE2', ' C 401  HOH  O  ', -0.408, (-25.749, 32.004, -47.132)), (' A 191 ALEU  C  ', ' A 191 ALEU HD22', -0.406, (-23.845, 3.258, -13.612)), (' A 191 BLEU  C  ', ' A 191 BLEU HD22', -0.406, (-23.845, 3.258, -13.612)), (' B  92  ILE HD13', ' B 185  LEU HD13', -0.404, (6.974, 1.919, -16.509))]
gui = coot_molprobity_todo_list_gui(data=data)