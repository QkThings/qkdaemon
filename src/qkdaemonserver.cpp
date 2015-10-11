/*
 * QkThings LICENSE
 * The open source framework and modular platform for smart devices.
 * Copyright (C) 2014 <http://qkthings.com>
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "qkdaemonserver.h"
#include "qkdaemonthread.h"
#include "qkdaemonsocket.h"


QkDaemonServer::QkDaemonServer(QkConnectionManager *connManager, QObject *parent) :
    QTcpServer(parent),
    m_connManager(connManager)
{
}


void QkDaemonServer::incomingConnection(qintptr socketDescriptor)
{
    qDebug() << "newConnection, socketDescriptor =" << socketDescriptor;

    QkDaemonThread *clientThread = new QkDaemonThread(socketDescriptor, m_connManager, this);
    connect(clientThread, SIGNAL(clientConnected(int)),
            this, SLOT(_handleClientConnected(int)));
    connect(clientThread, SIGNAL(clientDisconnected(int)),
            this, SLOT(_handleClientDisconnected(int)), Qt::DirectConnection);
    connect(clientThread, SIGNAL(finished()), clientThread, SLOT(deleteLater()));

    m_threads.insert(socketDescriptor, clientThread);
    clientThread->start();
}

void QkDaemonServer::_handleClientConnected(int socketDescriptor)
{
    QkDaemonSocket *socket = m_threads.value(socketDescriptor)->socket();
    qDebug() << "Client connected: " + socket->peerAddress().toString();
    emit info(tr("Client connected IP: ") + socket->peerAddress().toString());
}

void QkDaemonServer::_handleClientDisconnected(int socketDescriptor)
{
    QkDaemonSocket *socket = m_threads.value(socketDescriptor)->socket();
    qDebug() << "Client disconnected IP: " + socket->peerAddress().toString();
    emit info(tr("Client disconnected IP: ") + socket->peerAddress().toString());
    m_threads.remove(socket->socketDescriptor());
}
