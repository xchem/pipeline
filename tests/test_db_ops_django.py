import datetime
import os
import shutil
import unittest
import setup_django
from db.models import *
from functions import db_functions
from luigi_classes import db_ops_django
from test_functions import run_luigi_worker


class TestDataTransfer(unittest.TestCase):
    # filepath where test data is
    filepath = 'tests/docking_files/database/'
    db_file_name = 'soakDBDataFile.sqlite'
    # tmp directory to test in
    tmp_dir = 'tmp/'
    date = datetime.date.today()

    @classmethod
    def setUpClass(cls):
        cls.top_dir = os.getcwd()
        cls.working_dir = os.path.join(os.getcwd(), cls.tmp_dir)
        cls.db_full_path = os.path.join(cls.working_dir, cls.db_file_name)
        print('Working dir: ' + cls.working_dir)
        shutil.copytree(os.path.join(cls.top_dir, cls.filepath), cls.working_dir)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.working_dir)
        os.chdir(cls.top_dir)
        # delete rows created in soakdb table
        soakdb_rows = SoakdbFiles.objects.all()
        soakdb_rows.delete()
        # delete rows created in proposals table
        proposal_rows = Proposals.objects.all()
        proposal_rows.delete()

    def test_findsoakdb(self):
        os.chdir(self.working_dir)
        print(os.path.join(self.working_dir, self.filepath) + '/*')
        find_file = run_luigi_worker(db_ops_django.FindSoakDBFiles(
            filepath=str(os.path.join(self.working_dir, self.filepath) + '/*')))

        self.assertTrue(find_file)
        self.assertTrue(os.path.isfile(self.date.strftime('logs/soakDBfiles/soakDB_%Y%m%d.txt')))

    def test_transfer_fedids_files(self):
        transfer = run_luigi_worker(db_ops_django.TransferAllFedIDsAndDatafiles(
            date=self.date, soak_db_filepath=str(os.path.join(self.working_dir, self.filepath) + '/*')))
        self.assertTrue(transfer)

    def test_check_files(self):
        check_files = run_luigi_worker(db_ops_django.CheckFiles(self.date, soak_db_filepath=str(os.path.join
                                                                                                (self.working_dir,
                                                                                                 self.filepath)
                                                                                                + '/*')))
        self.assertTrue(check_files)

    def test_transfer_crystal(self):
        crystals_list = ['SHH-x100', 'SHH-x101', 'SHH-x102', 'SHH-x103', 'SHH-x104', 'SHH-x105', 'SHH-x106', 'SHH-x107',
                         'SHH-x108', 'SHH-x109', 'SHH-x110', 'SHH-x111', 'SHH-x112', 'SHH-x113', 'SHH-x114', 'SHH-x115',
                         'SHH-x116', 'SHH-x117', 'SHH-x118', 'SHH-x119', 'SHH-x120', 'SHH-x121', 'SHH-x122', 'SHH-x123',
                         'SHH-x124', 'SHH-x125', 'SHH-x126', 'SHH-x127', 'SHH-x128', 'SHH-x129', 'SHH-x130', 'SHH-x131',
                         'SHH-x132', 'SHH-x133', 'SHH-x134', 'SHH-x135', 'SHH-x136', 'SHH-x137', 'SHH-x138', 'SHH-x139',
                         'SHH-x140', 'SHH-x141', 'SHH-x142', 'SHH-x143', 'SHH-x144', 'SHH-x145', 'SHH-x146', 'SHH-x147',
                         'SHH-x148', 'SHH-x149', 'SHH-x150', 'SHH-x151', 'SHH-x152', 'SHH-x153', 'SHH-x154', 'SHH-x155',
                         'SHH-x156', 'SHH-x157', 'SHH-x158', 'SHH-x159', 'SHH-x160', 'SHH-x161', 'SHH-x162', 'SHH-x163',
                         'SHH-x164', 'SHH-x165', 'SHH-x166', 'SHH-x167', 'SHH-x168', 'SHH-x169', 'SHH-x170', 'SHH-x171',
                         'SHH-x172', 'SHH-x173', 'SHH-x174', 'SHH-x175', 'SHH-x176', 'SHH-x177', 'SHH-x178', 'SHH-x179',
                         'SHH-x180', 'SHH-x181', 'SHH-x182', 'SHH-x183', 'SHH-x184', 'SHH-x185', 'SHH-x186', 'SHH-x187',
                         'SHH-x188', 'SHH-x189', 'SHH-x190', 'SHH-x191', 'SHH-x192', 'SHH-x193', 'SHH-x194', 'SHH-x195',
                         'SHH-x196', 'SHH-x197', 'SHH-x198', 'SHH-x199', 'SHH-x200', 'SHH-x201', 'SHH-x202', 'SHH-x203',
                         'SHH-x204', 'SHH-x205', 'SHH-x206', 'SHH-x207', 'SHH-x208', 'SHH-x209', 'SHH-x210', 'SHH-x211',
                         'SHH-x212', 'SHH-x213', 'SHH-x214', 'SHH-x215', 'SHH-x216', 'SHH-x217', 'SHH-x218', 'SHH-x219',
                         'SHH-x220', 'SHH-x221', 'SHH-x222', 'SHH-x223', 'SHH-x224', 'SHH-x225', 'SHH-x226', 'SHH-x227',
                         'SHH-x228', 'SHH-x229', 'SHH-x230', 'SHH-x231', 'SHH-x232', 'SHH-x233', 'SHH-x234', 'SHH-x235',
                         'SHH-x236', 'SHH-x237', 'SHH-x238', 'SHH-x239', 'SHH-x240', 'SHH-x241', 'SHH-x242', 'SHH-x243',
                         'SHH-x244', 'SHH-x245', 'SHH-x246', 'SHH-x247', 'SHH-x248', 'SHH-x249', 'SHH-x250', 'SHH-x251',
                         'SHH-x252', 'SHH-x253', 'SHH-x254', 'SHH-x255', 'SHH-x256', 'SHH-x257', 'SHH-x258', 'SHH-x259',
                         'SHH-x260', 'SHH-x261', 'SHH-x262', 'SHH-x263', 'SHH-x264', 'SHH-x265', 'SHH-x266', 'SHH-x267',
                         'SHH-x268', 'SHH-x269', 'SHH-x270', 'SHH-x271', 'SHH-x272', 'SHH-x273', 'SHH-x274', 'SHH-x275',
                         'SHH-x276', 'SHH-x277', 'SHH-x278', 'SHH-x279', 'SHH-x280', 'SHH-x281', 'SHH-x282', 'SHH-x283',
                         'SHH-x284', 'SHH-x285', 'SHH-x286', 'SHH-x287', 'SHH-x288', 'SHH-x289', 'SHH-x290', 'SHH-x291',
                         'SHH-x292', 'SHH-x293', 'SHH-x294', 'SHH-x295', 'SHH-x296', 'SHH-x297', 'SHH-x298', 'SHH-x299',
                         'SHH-x300', 'SHH-x301', 'SHH-x302', 'SHH-x303', 'SHH-x304', 'SHH-x305', 'SHH-x306', 'SHH-x307',
                         'SHH-x308', 'SHH-x309', 'SHH-x310', 'SHH-x311', 'SHH-x312', 'SHH-x313', 'SHH-x314', 'SHH-x315',
                         'SHH-x316', 'SHH-x317', 'SHH-x318', 'SHH-x319', 'SHH-x320', 'SHH-x321', 'SHH-x322', 'SHH-x323',
                         'SHH-x324', 'SHH-x325', 'SHH-x326', 'SHH-x327', 'SHH-x328', 'SHH-x329', 'SHH-x330', 'SHH-x331',
                         'SHH-x332', 'SHH-x333', 'SHH-x334', 'SHH-x335', 'SHH-x336', 'SHH-x337', 'SHH-x338', 'SHH-x339',
                         'SHH-x340', 'SHH-x341', 'SHH-x342', 'SHH-x343', 'SHH-x344', 'SHH-x345', 'SHH-x346', 'SHH-x347',
                         'SHH-x348', 'SHH-x349', 'SHH-x350', 'SHH-x351', 'SHH-x352', 'SHH-x353', 'SHH-x354', 'SHH-x355',
                         'SHH-x356', 'SHH-x357', 'SHH-x358', 'SHH-x359', 'SHH-x360', 'SHH-x361', 'SHH-x362', 'SHH-x363',
                         'SHH-x364', 'SHH-x365', 'SHH-x366', 'SHH-x367', 'SHH-x368', 'SHH-x369', 'SHH-x370', 'SHH-x371',
                         'SHH-x372', 'SHH-x373', 'SHH-x374', 'SHH-x375', 'SHH-x376', 'SHH-x377', 'SHH-x378', 'SHH-x379',
                         'SHH-x380', 'SHH-x381', 'SHH-x382', 'SHH-x383', 'SHH-x384', 'SHH-x385', 'SHH-x386', 'SHH-x387',
                         'SHH-x388', 'SHH-x389', 'SHH-x390', 'SHH-x391', 'SHH-x392', 'SHH-x393', 'SHH-x394', 'SHH-x395',
                         'SHH-x396', 'SHH-x397', 'SHH-x398', 'SHH-x399', 'SHH-x400', 'SHH-x401', 'SHH-x402', 'SHH-x403',
                         'SHH-x404', 'SHH-x405', 'SHH-x406', 'SHH-x407', 'SHH-x408', 'SHH-x409', 'SHH-x410', 'SHH-x411',
                         'SHH-x412', 'SHH-x413', 'SHH-x414', 'SHH-x415', 'SHH-x416', 'SHH-x417', 'SHH-x418', 'SHH-x419',
                         'SHH-x420', 'SHH-x421', 'SHH-x422', 'SHH-x423', 'SHH-x424', 'SHH-x425', 'SHH-x426', 'SHH-x427',
                         'SHH-x428', 'SHH-x429', 'SHH-x430', 'SHH-x431', 'SHH-x432', 'SHH-x433', 'SHH-x434', 'SHH-x435',
                         'SHH-x436', 'SHH-x437', 'SHH-x438', 'SHH-x439', 'SHH-x440', 'SHH-x441', 'SHH-x442', 'SHH-x443',
                         'SHH-x444', 'SHH-x445', 'SHH-x446', 'SHH-x447', 'SHH-x448', 'SHH-x449', 'SHH-x450', 'SHH-x451',
                         'SHH-x452', 'SHH-x453', 'SHH-x454', 'SHH-x455', 'SHH-x456', 'SHH-x457', 'SHH-x458', 'SHH-x459',
                         'SHH-x460', 'SHH-x461', 'SHH-x462', 'SHH-x463', 'SHH-x464', 'SHH-x465', 'SHH-x466', 'SHH-x467',
                         'SHH-x468', 'SHH-x469', 'SHH-x470', 'SHH-x471', 'SHH-x472', 'SHH-x473', 'SHH-x474', 'SHH-x475',
                         'SHH-x476', 'SHH-x477', 'SHH-x478', 'SHH-x479', 'SHH-x480', 'SHH-x481', 'SHH-x482', 'SHH-x483',
                         'SHH-x484', 'SHH-x485', 'SHH-x486', 'SHH-x487', 'SHH-x488', 'SHH-x489', 'SHH-x490', 'SHH-x491',
                         'SHH-x492', 'SHH-x493', 'SHH-x494', 'SHH-x495', 'SHH-x496', 'SHH-x497', 'SHH-x498', 'SHH-x499',
                         'SHH-x500', 'SHH-x501', 'SHH-x502', 'SHH-x503', 'SHH-x504', 'SHH-x505', 'SHH-x506', 'SHH-x507',
                         'SHH-x508', 'SHH-x509', 'SHH-x510', 'SHH-x511', 'SHH-x512', 'SHH-x513', 'SHH-x514', 'SHH-x515',
                         'SHH-x516', 'SHH-x517', 'SHH-x518', 'SHH-x519', 'SHH-x520', 'SHH-x521', 'SHH-x522', 'SHH-x523',
                         'SHH-x524', 'SHH-x525', 'SHH-x526', 'SHH-x527', 'SHH-x528', 'SHH-x529', 'SHH-x530', 'SHH-x531',
                         'SHH-x532', 'SHH-x533', 'SHH-x534', 'SHH-x535', 'SHH-x536', 'SHH-x537', 'SHH-x538', 'SHH-x539',
                         'SHH-x540', 'SHH-x541', 'SHH-x542', 'SHH-x543', 'SHH-x544', 'SHH-x545', 'SHH-x546', 'SHH-x547',
                         'SHH-x548', 'SHH-x549', 'SHH-x550', 'SHH-x551', 'SHH-x552', 'SHH-x553', 'SHH-x554', 'SHH-x555',
                         'SHH-x556', 'SHH-x557', 'SHH-x558', 'SHH-x559', 'SHH-x560', 'SHH-x561', 'SHH-x562', 'SHH-x563',
                         'SHH-x564', 'SHH-x565', 'SHH-x566', 'SHH-x567', 'SHH-x568', 'SHH-x569', 'SHH-x570', 'SHH-x571',
                         'SHH-x572', 'SHH-x573', 'SHH-x574', 'SHH-x575', 'SHH-x576', 'SHH-x577', 'SHH-x578', 'SHH-x579',
                         'SHH-x580', 'SHH-x581', 'SHH-x582', 'SHH-x583', 'SHH-x584', 'SHH-x585', 'SHH-x586', 'SHH-x587',
                         'SHH-x588', 'SHH-x589', 'SHH-x590', 'SHH-x591', 'SHH-x592', 'SHH-x593', 'SHH-x594', 'SHH-x595',
                         'SHH-x596', 'SHH-x597', 'SHH-x598', 'SHH-x599', 'SHH-x600', 'SHH-x601', 'SHH-x602', 'SHH-x603',
                         'SHH-x604', 'SHH-x605', 'SHH-x606', 'SHH-x607', 'SHH-x608', 'SHH-x609', 'SHH-x610', 'SHH-x611',
                         'SHH-x612', 'SHH-x613', 'SHH-x614', 'SHH-x615', 'SHH-x616', 'SHH-x617', 'SHH-x618', 'SHH-x619',
                         'SHH-x620', 'SHH-x621', 'SHH-x622', 'SHH-x623', 'SHH-x624', 'SHH-x625', 'SHH-x626', 'SHH-x627',
                         'SHH-x628', 'SHH-x629', 'SHH-x630', 'SHH-x631', 'SHH-x632', 'SHH-x633', 'SHH-x634', 'SHH-x635',
                         'SHH-x636', 'SHH-x637', 'SHH-x638', 'SHH-x639', 'SHH-x640', 'SHH-x641', 'SHH-x642', 'SHH-x643',
                         'SHH-x644', 'SHH-x645', 'SHH-x646', 'SHH-x647', 'SHH-x648', 'SHH-x649', 'SHH-x650', 'SHH-x651',
                         'SHH-x652', 'SHH-x653', 'SHH-x654', 'SHH-x655', 'SHH-x656', 'SHH-x657', 'SHH-x658', 'SHH-x659',
                         'SHH-x660', 'SHH-x661', 'SHH-x662', 'SHH-x663', 'SHH-x664', 'SHH-x665', 'SHH-x666', 'SHH-x667',
                         'SHH-x668', 'SHH-x669', 'SHH-x670', 'SHH-x671', 'SHH-x672', 'SHH-x673', 'SHH-x674', 'SHH-x675',
                         'SHH-x676', 'SHH-x677', 'SHH-x678', 'SHH-x679', 'SHH-x680', 'SHH-x681', 'SHH-x682', 'SHH-x683',
                         'SHH-x684', 'SHH-x685', 'SHH-x686', 'SHH-x687', 'SHH-x688', 'SHH-x689', 'SHH-x690', 'SHH-x691',
                         'SHH-x692', 'SHH-x693', 'SHH-x694', 'SHH-x695', 'SHH-x696', 'SHH-x697', 'SHH-x698', 'SHH-x699',
                         'SHH-x700', 'SHH-x701', 'SHH-x702', 'SHH-x703', 'SHH-x704', 'SHH-x705', 'SHH-x706', 'SHH-x707',
                         'SHH-x708', 'SHH-x709', 'SHH-x710', 'SHH-x711', 'SHH-x712', 'SHH-x713', 'SHH-x714', 'SHH-x715',
                         'SHH-x716', 'SHH-x717', 'SHH-x718', 'SHH-x719', 'SHH-x720', 'SHH-x721', 'SHH-x722', 'SHH-x723',
                         'SHH-x724', 'SHH-x725', 'SHH-x726', 'SHH-x727', 'SHH-x728', 'SHH-x729', 'SHH-x730', 'SHH-x731',
                         'SHH-x732', 'SHH-x733', 'SHH-x734', 'SHH-x735', 'SHH-x736', 'SHH-x737', 'SHH-x738', 'SHH-x739',
                         'SHH-x740', 'SHH-x741', 'SHH-x742', 'SHH-x743', 'SHH-x744', 'SHH-x745', 'SHH-x746', 'SHH-x747',
                         'SHH-x748', 'SHH-x749', 'SHH-x750', 'SHH-x751', 'SHH-x752', 'SHH-x753', 'SHH-x754', 'SHH-x755',
                         'SHH-x756', 'SHH-x757', 'SHH-x758', 'SHH-x759', 'SHH-x760', 'SHH-x761', 'SHH-x762', 'SHH-x763',
                         'SHH-x764', 'SHH-x765', 'SHH-x766', 'SHH-x767', 'SHH-x768', 'SHH-x769', 'SHH-x770', 'SHH-x771',
                         'SHH-x772', 'SHH-x773', 'SHH-x774', 'SHH-x775', 'SHH-x776', 'SHH-x777', 'SHH-x778', 'SHH-x779',
                         'SHH-x780', 'SHH-x781', 'SHH-x782', 'SHH-x783', 'SHH-x784', 'SHH-x785', 'SHH-x786', 'SHH-x787',
                         'SHH-x788', 'SHH-x789', 'SHH-x790', 'SHH-x791', 'SHH-x792', 'SHH-x793', 'SHH-x794', 'SHH-x795',
                         'SHH-x796', 'SHH-x797', 'SHH-x798', 'SHH-x799', 'SHH-x800', 'SHH-x801', 'SHH-x802', 'SHH-x803',
                         'SHH-x804', 'SHH-x805', 'SHH-x806', 'SHH-x807', 'SHH-x808', 'SHH-x809', 'SHH-x810', 'SHH-x811',
                         'SHH-x812', 'SHH-x813', 'SHH-x814', 'SHH-x815', 'SHH-x816', 'SHH-x817', 'SHH-x818', 'SHH-x819',
                         'SHH-x820', 'SHH-x821', 'SHH-x822', 'SHH-x823', 'SHH-x824', 'SHH-x825', 'SHH-x826', 'SHH-x827',
                         'SHH-x828', 'SHH-x829', 'SHH-x830', 'SHH-x831', 'SHH-x832', 'SHH-x833', 'SHH-x834', 'SHH-x835',
                         'SHH-x836', 'SHH-x837', 'SHH-x838', 'SHH-x839', 'SHH-x840', 'SHH-x841', 'SHH-x842', 'SHH-x843',
                         'SHH-x844', 'SHH-x845', 'SHH-x846', 'SHH-x847', 'SHH-x848', 'SHH-x849', 'SHH-x850', 'SHH-x851',
                         'SHH-x852', 'SHH-x853', 'SHH-x854', 'SHH-x855', 'SHH-x856', 'SHH-x857', 'SHH-x858', 'SHH-x859',
                         'SHH-x860', 'SHH-x861', 'SHH-x862', 'SHH-x863', 'SHH-x864', 'SHH-x865', 'SHH-x866', 'SHH-x867',
                         'SHH-x868', 'SHH-x869', 'SHH-x870', 'SHH-x871', 'SHH-x872', 'SHH-x873', 'SHH-x874', 'SHH-x875',
                         'SHH-x876', 'SHH-x877', 'SHH-x878', 'SHH-x879', 'SHH-x880', 'SHH-x881', 'SHH-x882', 'SHH-x883',
                         'SHH-x884', 'SHH-x885', 'SHH-x886', 'SHH-x887', 'SHH-x888', 'SHH-x889', 'SHH-x890', 'SHH-x891',
                         'SHH-x892', 'SHH-x893', 'SHH-x894', 'SHH-x895', 'SHH-x896', 'SHH-x897', 'SHH-x898', 'SHH-x899',
                         'SHH-x900', 'SHH-x901', 'SHH-x902', 'SHH-x903', 'SHH-x904', 'SHH-x905', 'SHH-x906', 'SHH-x907',
                         'SHH-x908', 'SHH-x909', 'SHH-x910', 'SHH-x911', 'SHH-x912', 'SHH-x913', 'SHH-x914', 'SHH-x915',
                         'SHH-x916', 'SHH-x917', 'SHH-x918', 'SHH-x919', 'SHH-x920', 'SHH-x921', 'SHH-x922', 'SHH-x923',
                         'SHH-x924', 'SHH-x925', 'SHH-x926', 'SHH-x927', 'SHH-x928', 'SHH-x929', 'SHH-x930', 'SHH-x931',
                         'SHH-x932', 'SHH-x933', 'SHH-x934', 'SHH-x935', 'SHH-x936', 'SHH-x937', 'SHH-x938', 'SHH-x939',
                         'SHH-x940', 'SHH-x941', 'SHH-x942', 'SHH-x943', 'SHH-x944', 'SHH-x945', 'SHH-x946', 'SHH-x947',
                         'SHH-x948', 'SHH-x949', 'SHH-x950', 'SHH-x951', 'SHH-x952', 'SHH-x953', 'SHH-x954', 'SHH-x955',
                         'SHH-x956', 'SHH-x957', 'SHH-x958', 'SHH-x959', 'SHH-x960', 'SHH-x961', 'SHH-x962', 'SHH-x963',
                         'SHH-x964', 'SHH-x965', 'SHH-x966', 'SHH-x967', 'SHH-x968', 'SHH-x969', 'SHH-x970', 'SHH-x971',
                         'SHH-x972', 'SHH-x973', 'SHH-x974', 'SHH-x975', 'SHH-x976', 'SHH-x977', 'SHH-x978', 'SHH-x979',
                         'SHH-x980', 'SHH-x981', 'SHH-x982', 'SHH-x983', 'SHH-x984', 'SHH-x985', 'SHH-x986', 'SHH-x987',
                         'SHH-x988', 'SHH-x989', 'SHH-x990', 'SHH-x991', 'SHH-x992', 'SHH-x993', 'SHH-x994', 'SHH-x995',
                         'SHH-x996', 'SHH-x997', 'SHH-x998', 'SHH-x999', 'SHH-x1000', 'SHH-x1001', 'SHH-x1002',
                         'SHH-x1003', 'SHH-x1004', 'SHH-x1005', 'SHH-x1006', 'SHH-x1007', 'SHH-x1008', 'SHH-x1009',
                         'SHH-x1010', 'SHH-x1011', 'SHH-x1012', 'SHH-x1013', 'SHH-x1014', 'SHH-x1015', 'SHH-x1016',
                         'SHH-x1017', 'SHH-x1018', 'SHH-x1019', 'SHH-x1020', 'SHH-x1021', 'SHH-x1022', 'SHH-x1023',
                         'SHH-x1024', 'SHH-x1025', 'SHH-x1026', 'SHH-x1027', 'SHH-x1028', 'SHH-x1029', 'SHH-x1030',
                         'SHH-x1031', 'SHH-x1032', 'SHH-x1033', 'SHH-x1034', 'SHH-x1035', 'SHH-x1036', 'SHH-x1037',
                         'SHH-x1038', 'SHH-x1039', 'SHH-x1100', 'SHH-x1101', 'SHH-x1102', 'SHH-x1103', 'SHH-x1104',
                         'SHH-x1105', 'SHH-x1106', 'SHH-x1107', 'SHH-x1108', 'SHH-x1109', 'SHH-x1110', 'SHH-x1111',
                         'SHH-x1112', 'SHH-x1113', 'SHH-x1114', 'SHH-x1115', 'SHH-x1116', 'SHH-x1117', 'SHH-x1118',
                         'SHH-x1119', 'SHH-x1120', 'SHH-x1121', 'SHH-x1122', 'SHH-x1123', 'SHH-x1124', 'SHH-x1125',
                         'SHH-x1126', 'SHH-x1127', 'SHH-x1128', 'SHH-x1200', 'SHH-x1201', 'SHH-x1202', 'SHH-x1203',
                         'SHH-x1204', 'SHH-x1205', 'SHH-x1206', 'SHH-x1207', 'SHH-x1208', 'SHH-x1209', 'SHH-x1210',
                         'SHH-x1211', 'SHH-x1212', 'SHH-x1213', 'SHH-x1214', 'SHH-x1215', 'SHH-x1216', 'SHH-x1217',
                         'SHH-x1218', 'SHH-x1219', 'SHH-x1220', 'SHH-x1221', 'SHH-x1222', 'SHH-x1223', 'SHH-x1224',
                         'SHH-x1225', 'SHH-x1226', 'SHH-x1227', 'SHH-x1228', 'SHH-x1229', 'SHH-x1230', 'SHH-x1231',
                         'SHH-x1232', 'SHH-x1233', 'SHH-x1234', 'SHH-x1235', 'SHH-x1236', 'SHH-x1237', 'SHH-x1238',
                         'SHH-x1239', 'SHH-x1240', 'SHH-x1241', 'SHH-x1242', 'SHH-x1243', 'SHH-x1244', 'SHH-x1245',
                         'SHH-x1246', 'SHH-x1247', 'SHH-x1248', 'SHH-x1249', 'SHH-x1250', 'SHH-x1251', 'SHH-x1252',
                         'SHH-x1253', 'SHH-x1254', 'SHH-x1255', 'SHH-x1256', 'SHH-x1257', 'SHH-x1258', 'SHH-x1259',
                         'SHH-x1260', 'SHH-x1261', 'SHH-x1262', 'SHH-x1263', 'SHH-x1264', 'SHH-x1265', 'SHH-x1266',
                         'SHH-x1267', 'SHH-x1268', 'SHH-x1269', 'SHH-x1270', 'SHH-x1271', 'SHH-x1272', 'SHH-x1273',
                         'SHH-x1274', 'SHH-x1275', 'SHH-x1276', 'SHH-x1277', 'SHH-x1278', 'SHH-x1279', 'SHH-x1280',
                         'SHH-x1281', 'SHH-x1282', 'SHH-x1283', 'SHH-x1284', 'SHH-x1285', 'SHH-x1286', 'SHH-x1287',
                         'SHH-x1288', 'SHH-x1289', 'SHH-x1290', 'SHH-x1291', 'SHH-x1292', 'SHH-x1293', 'SHH-x1294',
                         'SHH-x1295', 'SHH-x1296', 'SHH-x1297', 'SHH-x1298', 'SHH-x1299', 'SHH-x1300', 'SHH-x1301',
                         'SHH-x1302', 'SHH-x1303', 'SHH-x1304', 'SHH-x1305', 'SHH-x1306', 'SHH-x1307', 'SHH-x1308',
                         'SHH-x1309', 'SHH-x1310', 'SHH-x1311', 'SHH-x1312', 'SHH-x1313', 'SHH-x1314', 'SHH-x1315',
                         'SHH-x1316', 'SHH-x1317', 'SHH-x1318', 'SHH-x1319', 'SHH-x1320', 'SHH-x1321', 'SHH-x1322',
                         'SHH-x1323', 'SHH-x1324', 'SHH-x1325', 'SHH-x1326', 'SHH-x1327', 'SHH-x1328', 'SHH-x1329',
                         'SHH-x1330', 'SHH-x1331', 'SHH-x1332', 'SHH-x1333', 'SHH-x1334', 'SHH-x1335', 'SHH-x1336',
                         'SHH-x1337', 'SHH-x1338', 'SHH-x1339', 'SHH-x1340', 'SHH-x1341', 'SHH-x1342', 'SHH-x1343',
                         'SHH-x1344', 'SHH-x1345', 'SHH-x1346', 'SHH-x1347', 'SHH-x1348', 'SHH-x1349', 'SHH-x1350',
                         'SHH-x1351', 'SHH-x1352', 'SHH-x1353', 'SHH-x1354', 'SHH-x1355', 'SHH-x1356', 'SHH-x1357',
                         'SHH-x1358', 'SHH-x1359', 'SHH-x1360', 'SHH-x1361', 'SHH-x1362', 'SHH-x1363', 'SHH-x1364',
                         'SHH-x1365', 'SHH-x1366', 'SHH-x1367', 'SHH-x1368', 'SHH-x1369', 'SHH-x1370', 'SHH-x1371',
                         'SHH-x1372', 'SHH-x1373', 'SHH-x1374', 'SHH-x1375', 'SHH-x1376', 'SHH-x1377', 'SHH-x1378',
                         'SHH-x1379', 'SHH-x1380', 'SHH-x1381', 'SHH-x1382', 'SHH-x1383', 'SHH-x1384', 'SHH-x1385',
                         'SHH-x1386', 'SHH-x1387', 'SHH-x1388', 'SHH-x1389', 'SHH-x1390', 'SHH-x1391', 'SHH-x1392',
                         'SHH-x1393', 'SHH-x1394', 'SHH-x1395', 'SHH-x1396', 'SHH-x1397', 'SHH-x1398', 'SHH-x1399',
                         'SHH-x1400', 'SHH-x1401', 'SHH-x1402', 'SHH-x1403', 'SHH-x1404', 'SHH-x1405', 'SHH-x1406',
                         'SHH-x1407', 'SHH-x1500', 'SHH-x1501', 'SHH-x1502', 'SHH-x1503', 'SHH-x1504', 'SHH-x1505',
                         'SHH-x1506', 'SHH-x1507', 'SHH-x1508', 'SHH-x1509', 'SHH-x1510', 'SHH-x1511', 'SHH-x1512',
                         'SHH-x1513', 'SHH-x1514', 'SHH-x1515', 'SHH-x1516', 'SHH-x1517', 'SHH-x1518', 'SHH-x1519',
                         'SHH-x1520', 'SHH-x1521', 'SHH-x1522', 'SHH-x1523', 'SHH-x1524', 'SHH-x1525', 'SHH-x1526',
                         'SHH-x1527', 'SHH-x1528', 'SHH-x1529', 'SHH-x1530', 'SHH-x1531', 'SHH-x1532', 'SHH-x1533',
                         'SHH-x1534', 'SHH-x1535', 'SHH-x1536', 'SHH-x1537', 'SHH-x1538', 'SHH-x1539', 'SHH-x1540',
                         'SHH-x1541', 'SHH-x1542', 'SHH-x1543', 'SHH-x1544', 'SHH-x1545', 'SHH-x1546', 'SHH-x1547',
                         'SHH-x1548', 'SHH-x1549', 'SHH-x1550', 'SHH-x1551', 'SHH-x1552', 'SHH-x1553', 'SHH-x1554',
                         'SHH-x1555', 'SHH-x1556', 'SHH-x1557', 'SHH-x1558', 'SHH-x1559', 'SHH-x1560', 'SHH-x1561',
                         'SHH-x1562', 'SHH-x1563', 'SHH-x1564', 'SHH-x1565', 'SHH-x1566', 'SHH-x1567', 'SHH-x1568',
                         'SHH-x1569', 'SHH-x1570', 'SHH-x1571', 'SHH-x1572', 'SHH-x1573', 'SHH-x1574', 'SHH-x1575',
                         'SHH-x1576', 'SHH-x1577', 'SHH-x1578', 'SHH-x1579', 'SHH-x1580', 'SHH-x1581', 'SHH-x1582',
                         'SHH-x1583', 'SHH-x1584', 'SHH-x1585', 'SHH-x1586', 'SHH-x1587', 'SHH-x1588', 'SHH-x1589',
                         'SHH-x1590', 'SHH-x1591', 'SHH-x1592', 'SHH-x1593', 'SHH-x1594', 'SHH-x1595', 'SHH-x1596',
                         'SHH-x1597', 'SHH-x1598', 'SHH-x1599', 'SHH-x1600', 'SHH-x1601', 'SHH-x1602', 'SHH-x1603',
                         'SHH-x1604', 'SHH-x1605', 'SHH-x1606', 'SHH-x1607', 'SHH-x1608', 'SHH-x1609', 'SHH-x1610',
                         'SHH-x1611', 'SHH-x1612', 'SHH-x1613', 'SHH-x1614', 'SHH-x1615', 'SHH-x1616', 'SHH-x1617',
                         'SHH-x1618', 'SHH-x1619', 'SHH-x1620', 'SHH-x1621', 'SHH-x1622', 'SHH-x1623', 'SHH-x1624',
                         'SHH-x1625', 'SHH-x1626', 'SHH-x1627', 'SHH-x1628', 'SHH-x1629', 'SHH-x1630', 'SHH-x1631',
                         'SHH-x1632', 'SHH-x1633', 'SHH-x1634', 'SHH-x1635', 'SHH-x1636', 'SHH-x1637', 'SHH-x1638',
                         'SHH-x1639', 'SHH-x1640', 'SHH-x1641', 'SHH-x1642', 'SHH-x1643', 'SHH-x1644', 'SHH-x1645',
                         'SHH-x1646', 'SHH-x1647', 'SHH-x1648', 'SHH-x1649', 'SHH-x1650', 'SHH-x1651', 'SHH-x1652',
                         'SHH-x1653', 'SHH-x1654', 'SHH-x1655', 'SHH-x1656', 'SHH-x1657', 'SHH-x1658', 'SHH-x1659',
                         'SHH-x1660', 'SHH-x1661', 'SHH-x1662', 'SHH-x1', 'SHH-x10', 'SHH-x11', 'SHH-x12', 'SHH-x13',
                         'SHH-x14', 'SHH-x15', 'SHH-x16', 'SHH-x17', 'SHH-x18', 'SHH-x19', 'SHH-x2', 'SHH-x20',
                         'SHH-x21', 'SHH-x22', 'SHH-x23', 'SHH-x24', 'SHH-x25', 'SHH-x26', 'SHH-x27', 'SHH-x28',
                         'SHH-x29', 'SHH-x3', 'SHH-x30', 'SHH-x31', 'SHH-x32', 'SHH-x33', 'SHH-x34', 'SHH-x35',
                         'SHH-x36', 'SHH-x37', 'SHH-x38', 'SHH-x39', 'SHH-x4', 'SHH-x40', 'SHH-x41', 'SHH-x42',
                         'SHH-x43', 'SHH-x44', 'SHH-x45', 'SHH-x46', 'SHH-x47', 'SHH-x48', 'SHH-x49', 'SHH-x5',
                         'SHH-x50', 'SHH-x51', 'SHH-x52', 'SHH-x53', 'SHH-x54', 'SHH-x55', 'SHH-x56', 'SHH-x57',
                         'SHH-x58', 'SHH-x59', 'SHH-x6', 'SHH-x60', 'SHH-x61', 'SHH-x62', 'SHH-x63', 'SHH-x64',
                         'SHH-x65', 'SHH-x66', 'SHH-x67', 'SHH-x68', 'SHH-x69', 'SHH-x7', 'SHH-x70', 'SHH-x71',
                         'SHH-x72', 'SHH-x73', 'SHH-x74', 'SHH-x75', 'SHH-x76', 'SHH-x77', 'SHH-x78', 'SHH-x8',
                         'SHH-x9',
                         'coot-download', 'coot-backup', 'coot-refmac']

        results = db_functions.soakdb_query(self.db_full_path)
        db_functions.transfer_table(translate_dict=db_functions.crystal_translations(), results=results, model=Crystal)
        crystals = Crystal.objects.values_list('crystal_name', flat=True)
        self.assertTrue(set(crystals) == set(crystals_list))
