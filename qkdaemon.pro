#-------------------------------------------------
#
# Project created by QtCreator 2013-07-22T20:19:20
#
#-------------------------------------------------

QT      += core network
QT      += gui

greaterThan(QT_MAJOR_VERSION, 4): QT += widgets

CONFIG += console

TARGET = qkdaemon
TEMPLATE = app

#DEFINES += QT_NO_DEBUG_OUTPUT

INCLUDEPATH += ../utils
INCLUDEPATH += src/ src/gui

SOURCES += src/main.cpp\
        src/gui/mainwindow.cpp \
    src/gui/aboutdialog.cpp \
 #   src/qkdaemon.cpp \
 #   src/qkdaemonthread.cpp \
 #   src/qkdaemonserver.cpp \
 #   src/qkdaemonsocket.cpp \
 #   src/qkapihandler.cpp \
    src/clhandler.cpp \
    ../utils/qkgui.cpp \
    src/qkdaemon.cpp \
    src/qkconnmanager.cpp \
    src/qkconnworker.cpp \
    ../utils/qkcore.cpp \
    ../utils/qkutils.cpp

HEADERS  += src/gui/qkdaemon_gui.h \
    src/gui/mainwindow.h \
    src/gui/aboutdialog.h \
 #   src/qkdaemon.h \
 #   src/qkdaemonthread.h \
 #   src/qkdaemonserver.h \
 #   src/qkdaemonsocket.h \
    src/clhandler.h \
    ../utils/qkgui.h \
    src/qkdaemon.h \
    src/qkconnmanager.h \
    src/qkconnworker.h \
    ../utils/qkcore.h \
    ../utils/qkutils.h

FORMS    += src/gui/mainwindow.ui \
    src/gui/aboutdialog.ui

RESOURCES += \
    resources/resources.qrc

CONFIG(debug, debug|release) {
    DESTDIR = debug
} else {
    DESTDIR = release
}

OBJECTS_DIR = build/obj
MOC_DIR = build/moc
RCC_DIR = build/rcc
UI_DIR = build/ui
