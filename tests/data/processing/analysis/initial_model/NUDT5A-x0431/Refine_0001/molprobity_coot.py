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
data['rama'] = [('A', '  66 ', 'PRO', 0.031914314833096306, (-14.29, -7.740000000000001, -27.15)), ('B', '  66 ', 'PRO', 0.07070742000834286, (-1.4609999999999999, 13.878, -17.014)), ('C', '  24 ', 'SER', 0.028926633000085546, (-16.283, 16.792, -66.724)), ('C', '  66 ', 'PRO', 0.03246234515550537, (-36.25299999999999, 27.748999999999995, -38.186)), ('D', ' 190 ', 'HIS', 0.0036956677082151056, (-22.716, 2.1099999999999985, -42.635))]
data['omega'] = []
data['rota'] = [('A', '  14 ', 'LYS', 0.015125138592456015, (15.291, -0.6060000000000001, -2.2320000000000007)), ('A', '  43 ', 'THR', 0.028558605117723346, (10.189, -0.5920000000000001, 8.8)), ('A', '  70 ', 'ARG', 0.011721445643186291, (-19.553000000000004, -6.932, -38.768)), ('A', ' 125 ', 'GLU', 0.22730451033080185, (-3.5880000000000005, -10.007000000000001, -27.845000000000002)), ('A', ' 191 ', 'LEU', 0.00041415490614677103, (-25.087999999999997, 2.6659999999999995, -12.399000000000003)), ('B', '  38 ', 'ASP', 0.03015595749420438, (-30.545000000000012, -12.579, -6.4670000000000005)), ('B', '  43 ', 'THR', 0.042942968235854154, (-30.85100000000001, -14.870999999999999, -0.947)), ('B', ' 205 ', 'LYS', 0.13783724332044076, (-3.5330000000000004, 1.7049999999999983, -30.226)), ('B', ' 208 ', 'ASN', 0.0835119516518455, (-6.678000000000001, 2.9299999999999997, -35.193)), ('C', '  31 ', 'LEU', 0.15393326372578148, (-19.801000000000002, 14.71, -63.53200000000001)), ('C', '  43 ', 'THR', 0.1004718377744824, (-20.003, -5.2280000000000015, -62.809)), ('C', '  53 ', 'THR', 0.1160225684179532, (-16.37, 22.420000000000005, -52.8)), ('C', '  98 ', 'LEU', 0.0, (-22.141, 26.09600000000001, -47.077)), ('C', '  98 ', 'LEU', 0.0, (-22.141, 26.09600000000001, -47.077)), ('D', '  24 ', 'SER', 0.14844115152227058, (-21.109, 18.175000000000004, -70.904)), ('D', '  43 ', 'THR', 0.0845621650041976, (-25.53, 39.388999999999996, -65.601)), ('D', '  45 ', 'THR', 0.2167696384910837, (-23.561999999999998, 32.84100000000001, -64.183)), ('D', '  72 ', 'LEU', 0.03459300639199396, (-55.93000000000002, -2.4010000000000007, -33.875)), ('D', ' 196 ', 'ARG', 0.0, (-36.75099999999998, 13.854, -47.321)), ('D', ' 196 ', 'ARG', 0.0, (-36.75099999999998, 13.854, -47.321)), ('D', ' 208 ', 'ASN', 0.0, (-46.535, 18.48, -31.105))]
data['cbeta'] = [('A', '  82 ', 'GLN', ' ', 0.29104127738907676, (-23.241, -7.879, -13.328000000000001)), ('B', '  38 ', 'ASP', ' ', 0.2955510141672549, (-29.772000000000006, -13.908, -6.572))]
data['probe'] = [(' D 196 BARG  CG ', ' D 196 BARG HH21', -1.052, (-36.82, 15.316, -50.432)), (' D 196 DARG  CG ', ' D 196 DARG HH21', -1.052, (-36.82, 15.316, -50.432)), (' D 196 DARG  HG3', ' D 196 DARG  NH2', -1.046, (-35.846, 15.565, -50.432)), (' D 196 BARG  HG3', ' D 196 BARG  NH2', -1.046, (-35.846, 15.565, -50.432)), (' D 196 DARG  HG3', ' D 196 DARG HH21', -0.93, (-36.999, 15.275, -49.71)), (' D 196 BARG  HG3', ' D 196 BARG HH21', -0.93, (-36.999, 15.275, -49.71)), (' C 125  GLU  OE1', ' C 403  HOH  O  ', -0.81, (-35.647, 17.993, -33.343)), (' C 104  THR HG23', ' C 107  ALA  H  ', -0.795, (-21.891, 25.207, -36.433)), (' B  92  ILE HD11', ' B 191  LEU HD13', -0.777, (8.332, 0.236, -14.801)), (' D 196 BARG  CG ', ' D 196 BARG  NH2', -0.758, (-36.083, 14.074, -49.479)), (' D 196 DARG  CG ', ' D 196 DARG  NH2', -0.758, (-36.083, 14.074, -49.479)), (' D 120  LYS  H  ', ' D 155  ASN HD21', -0.72, (-49.94, -4.383, -51.17)), (' A 203  ALA  HB3', ' B 203  ALA  HB3', -0.684, (-7.717, 3.993, -25.278)), (' A 206  HIS  HD2', ' B 200  TYR  OH ', -0.668, (-5.611, 10.569, -23.131)), (' B 104  THR HG23', ' B 107  ALA  H  ', -0.649, (-10.977, 19.518, -7.468)), (' C 203  ALA  HB3', ' D 203  ALA  HB3', -0.61, (-42.866, 16.282, -41.043)), (' B 301  EDO  C2 ', ' B 404  HOH  O  ', -0.604, (2.979, 23.271, -26.13)), (' D 189  GLU  O  ', ' D 190  HIS  C  ', -0.599, (-24.833, 1.758, -43.101)), (' D 104  THR HG23', ' D 107  ALA  H  ', -0.584, (-50.682, 7.945, -62.284)), (' C 132  MET  HE1', ' C 196  ARG  CZ ', -0.584, (-32.576, 20.117, -50.308)), (' D 189  GLU  O  ', ' D 191  LEU  HG ', -0.563, (-26.367, 2.083, -42.405)), (' A 104  THR HG23', ' A 107  ALA  H  ', -0.562, (-5.877, -19.594, -22.546)), (' A 125  GLU  OE1', ' B 206  HIS  HE1', -0.556, (-5.018, -4.147, -29.375)), (' B 301  EDO  H22', ' B 404  HOH  O  ', -0.554, (2.742, 23.406, -26.63)), (' C 403  HOH  O  ', ' D 206  HIS  HE1', -0.548, (-36.383, 19.109, -33.687)), (' C 203  ALA  HB2', ' D 200  TYR  CD1', -0.548, (-43.983, 13.597, -42.825)), (' A  54  ARG  HD3', ' A  60  ASP  OD1', -0.543, (-4.739, -13.713, -7.505)), (' A 104  THR  CG2', ' A 107  ALA  H  ', -0.53, (-5.972, -19.838, -22.556)), (' C 132  MET  O  ', ' D 196 DARG  NH1', -0.53, (-32.856, 13.9, -51.382)), (' C 179  LEU HD11', ' C 198  TYR  CZ ', -0.526, (-47.964, 19.364, -50.215)), (' C 206  HIS  HD2', ' D 200  TYR  OH ', -0.522, (-47.15, 11.382, -43.94)), (' C 132  MET  O  ', ' D 196 BARG  NH1', -0.516, (-32.803, 14.505, -51.732)), (' C 200  TYR  OH ', ' D 206  HIS  HD2', -0.511, (-39.636, 20.743, -37.518)), (' D 152  ASP  OD2', ' D 154  GLU  N  ', -0.509, (-54.806, -8.571, -50.378)), (' D 194  ASP  OD1', ' D 196 AARG  HD3', -0.509, (-34.803, 12.016, -49.684)), (' D 194  ASP  OD1', ' D 196 CARG  HD3', -0.509, (-34.803, 12.016, -49.684)), (' D  71  THR  O  ', ' D  73  HIS  ND1', -0.504, (-54.415, -4.265, -36.373)), (' A 132  MET  HE1', ' A 196  ARG  CZ ', -0.495, (-10.688, -4.375, -13.595)), (' A 200  TYR  OH ', ' B 206  HIS  HD2', -0.491, (-9.081, -1.58, -27.886)), (' D 188  GLU  HG3', ' D 189  GLU  HG3', -0.488, (-27.035, -0.587, -37.663)), (' A  39  PRO  HG2', ' B 167  PHE  CD2', -0.486, (11.63, 6.159, 0.139)), (' C 112  GLU  OE2', ' C 401  HOH  O  ', -0.483, (-26.332, 31.806, -47.126)), (' C 194  ASP  OD2', ' C 196  ARG  NH2', -0.481, (-33.993, 21.973, -51.066)), (' A 179  LEU HD23', ' A 205  LYS  HD2', -0.48, (-14.484, 11.754, -24.433)), (' A 191  LEU  C  ', ' A 191  LEU HD23', -0.479, (-23.692, 3.317, -14.043)), (' B  97 DGLY  HA3', ' B 112  GLU  HG3', -0.477, (-4.171, 11.661, -4.692)), (' B 189  GLU  HB3', ' B 191  LEU HD12', -0.477, (9.986, -1.659, -14.835)), (' A 132  MET  O  ', ' B 196  ARG  NH2', -0.474, (-6.677, -0.807, -10.687)), (' D  64  VAL  O  ', ' D  94  PHE  HB3', -0.471, (-41.675, 7.058, -50.342)), (' C  51 CARG  NH2', ' F   1 CHOH  O  ', -0.469, (-25.755, 19.604, -51.775)), (' C  51 DARG  NH2', ' F   1 DHOH  O  ', -0.469, (-25.755, 19.604, -51.775)), (' B  97 CGLY  HA3', ' B 112  GLU  HG3', -0.468, (-4.381, 12.059, -4.973)), (' C  15  GLN  N  ', ' C 404  HOH  O  ', -0.466, (-21.396, -5.913, -52.884)), (' B  17  ILE HD12', ' B  34  THR  CG2', -0.466, (-24.599, -6.028, -0.44)), (' A 200  TYR  CD1', ' B 203  ALA  HB2', -0.464, (-9.095, 0.942, -24.92)), (' C  97 DGLY  HA3', ' C 112  GLU  CD ', -0.456, (-25.324, 29.275, -46.555)), (' C 200  TYR  CD1', ' D 203  ALA  HB2', -0.451, (-41.076, 18.739, -40.656)), (' C 131  CYS  SG ', ' D 194  ASP  HA ', -0.448, (-30.968, 10.363, -48.069)), (' B 158  PRO  O  ', ' B 160  PRO  HD3', -0.447, (10.071, 18.155, -9.331)), (' A  71  THR  O  ', ' A  73  HIS  HD2', -0.445, (-22.409, -4.732, -41.316)), (' D  35  THR HG23', ' D  43  THR HG23', -0.443, (-24.882, 36.212, -67.64)), (' C  97 CGLY  HA3', ' C 112  GLU  CD ', -0.443, (-25.563, 29.561, -46.241)), (' C  22  LEU  C  ', ' C  22  LEU HD13', -0.435, (-13.905, 13.012, -65.82)), (' C 125  GLU  HG2', ' C 402  HOH  O  ', -0.428, (-31.925, 16.672, -34.984)), (' B  98 CLEU HD11', ' E   2 CLIG  C1 ', -0.421, (-6.84, 6.333, 1.887)), (' B  98 DLEU HD11', ' E   2 DLIG  C1 ', -0.421, (-6.84, 6.333, 1.887)), (' D 188  GLU  CG ', ' D 189  GLU  HG3', -0.419, (-27.208, -0.241, -37.616)), (' C  81  LYS  HA ', ' C  91  CYS  O  ', -0.418, (-37.787, 30.821, -52.382)), (' B  58  THR  CB ', ' B 142  HIS  NE2', -0.412, (-16.608, 12.24, -7.047)), (' C 136  LEU  O  ', ' C 136  LEU HD13', -0.412, (-26.458, 13.304, -59.336)), (' D  81  LYS  HA ', ' D  91  CYS  O  ', -0.408, (-31.981, 2.011, -48.595))]
gui = coot_molprobity_todo_list_gui(data=data)